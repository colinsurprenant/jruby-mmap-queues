# encoding: utf-8

require "thread"
require "forwardable"
require "jruby-mmap"

class PageCache

  def initialize(cache_size, page_base_fname, page_size)
    @cache_size = cache_size
    @page_base_fname = page_base_fname
    @page_size = page_size
    @cache = {}
    @num_pages = 0
  end

  def page(index)
    @cache[index] || add(index)
  end

  def close
    @cache.values.each{|page| page.close}
  end

  private

  def add(index)
    # TBD uncache, LRU?
    @num_pages += 1
    page = Mmap.new("#{@page_base_fname}.#{index}", @page_size)
    page.load
    @cache[index] = page
  end
end

class MappedMetadata

  # meta structure
  # 00: head page index  (long 8 bytes)
  # 08: head page offset (int  4 bytes)
  # 12: tail page index  (long 8 bytes)
  # 20: tail page offset (int  4 bytes)

  def initialize(fname)
    @meta = Mmap.new(fname, 2048) # you know, the constant
    @meta.load
  end

  def head_page_index
    @head_page_index ||= @meta.get_long_at(0)
  end

  def head_page_index=(index)
    @meta.put_long_at(0, index)
    @head_page_index = index
  end

  def head_page_offset
    @head_page_offset ||= @meta.get_int_at(8)
  end

  def head_page_offset=(offset)
    @meta.put_int_at(8, offset)
    @head_page_offset = offset
  end

  def tail_page_index
    @tail_page_index ||= @meta.get_long_at(12)
  end

  def tail_page_index=(index)
    @meta.put_long_at(12, index)
    @tail_page_index = index
  end

  def tail_page_offset
    @tail_page_offset ||= @meta.get_int_at(20)
  end

  def tail_page_offset=(offset)
    @meta.put_int_at(20, offset)
    @tail_page_offset = offset
  end

  def close
    @meta.close
  end
end

# PagedQueue
# non blocking, non thread-safe persistent queue implementation using mmap
class PagedQueue
  INT_BYTES = 4
  LONG_BYTES = 8

  # data structure
  # 0: data size (int  4 bytes)
  # 4: data (data size in bytes)
  # ...
  #
  # a data size of 0 signal the end of the page

  def initialize(fname, page_size)
    @fname = fname
    @page_size = page_size
    @page_max_data_size = @page_size - (2 * INT_BYTES)

    @meta = MappedMetadata.new(@fname)
    @cache = PageCache.new(2, @fname, @page_size)

    # prime the cache with tail & head pages
    @cache.page(@meta.head_page_index)
    @cache.page(@meta.tail_page_index)
  end

  # @param data [String] write the data string backing bytes to queue
  # @return [Integer] number of data bytes written
  def push(data)
    size = data.bytesize
    raise("data size=#{data.bytesize} is larger than configured page usable size=#{@page_max_data_size} (#{@page_size} - #{2 * INT_BYTES})") if size > @page_max_data_size

    offset = @meta.head_page_offset
    if size > (@page_max_data_size - offset)
      # data size is larger than usable data size left, move head to next page
      @meta.head_page_index += 1
      @meta.head_page_offset = offset = 0
    end
    page = @cache.page(@meta.head_page_index)
    page.position = offset
    page.put_int(size)
    page.put_bytes(data)

    # write a trailing 0 after the data. on next push, this will be overwritten with next data size
    # since offset will be set on this trailing zero, otherwise if we changed page, trailing zero
    # will be set.
    page.put_int(0)
    @meta.head_page_offset = offset + size + INT_BYTES

    size
  end
  alias_method :<<, :push

  # skip is like pop but does not read the data, just skip over it
  # @return [Integer] number of data bytes skipped
  def skip
    offset = @meta.tail_page_offset
    index = @meta.tail_page_index
    return nil if index >= @meta.head_page_index && offset >= @meta.head_page_offset
    page = @cache.page(index)

    page.position = offset

    if (size = page.get_int) == 0
      # we hit the trailing zero that indicates the end of data on this page
      # TBD refactor, duplicate code
      offset = @meta.tail_page_offset = 0
      index = @meta.tail_page_index += 1
      return nil if index >= @meta.head_page_index && offset >= @meta.head_page_offset
      page = @cache.page(index)
      page.position = offset
      size = page.get_int
    end

    @meta.tail_page_offset = offset + size + INT_BYTES

    size
  end

  # @return [String] retrieve data bytes from queue
  def pop
    offset = @meta.tail_page_offset
    index = @meta.tail_page_index
    return nil if index >= @meta.head_page_index && offset >= @meta.head_page_offset
    page = @cache.page(index)

    page.position = offset

    if (size = page.get_int) == 0
      # we hit the trailing zero that indicates the end of data on this page
      # TBD refactor, duplicate code with skip
      offset = @meta.tail_page_offset = 0
      index = @meta.tail_page_index += 1
      return nil if index >= @meta.head_page_index && offset >= @meta.head_page_offset
      page = @cache.page(index)
      page.position = offset
      size = page.get_int
    end

    @meta.tail_page_offset = offset + size + INT_BYTES
    page.get_bytes(size)
  end
  alias_method :shift, :pop

  def close
    @meta.close
    @cache.close
  end
end


# MappedSizedQueue blocking thread-safe sized queue
# uses both an in-memory queue and a persistent queue and pushes to both
# but pops from the in-memory queue.
class MappedSizedQueue

  # @param fname [String] the queue base file name
  # @param size [Integer] the queue max size
  # @param options [Hash]
  # @option options [Boolean] :debug, default to false
  # @option options [Boolean] :seralize, serialize to json, default to true
  def initialize(fname, size, options = {})
    options = {:debug => false, :serialize => true}.merge(options)
    raise(ArgumentError, "queue size must be positive") unless size > 0

    @serialize = options.fetch(:serialize)
    @size = size

    # @mq is the in-memory queue and @pq the persistent queue
    @mq = []
    @pq = PagedQueue.new(fname, 100 * 1024 * 1024)

    @num_pop_waiting = 0
    @num_push_waiting = 0
    @mutex = Mutex.new
    @non_empty = ConditionVariable.new
    @non_full = ConditionVariable.new
  end

  def empty?
    @mutex.synchronize{@mq.empty?}
  end

  def push(data)
    @mutex.synchronize do
      while true
        break if @mq.length < @size
        @num_push_waiting += 1
        begin
          @non_full.wait(@mutex)
        ensure
          @num_push_waiting -= 1
        end
      end

      @pq.push(serialize(data))
      @mq.push(data)

      @non_empty.signal
    end
  end
  alias_method :<<, :push

  def pop
    data = memory_pop

    @mutex.synchronize do
      if @mq.length < @size
        @non_full.signal
      end
    end

    data
  end
  alias_method :shift, :pop

  def close
    @pq.close
  end

  def purge
    raise("unimplemented")
  end

  private

  def memory_pop(non_block = false)
    @mutex.synchronize do
      while true
        unless @mq.empty?
          @pq.skip
          return @mq.shift
        end
        raise(ThreadError, "queue empty") if non_block

        begin
          @num_pop_waiting += 1
          @non_empty.wait(@mutex)
        ensure
          @num_pop_waiting -= 1
        end
      end
    end
  end

  def serialize(data)
    data
  end

  def deserialize(data)
    data
  end
end
