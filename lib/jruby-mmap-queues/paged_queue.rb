# encoding: utf-8

require "jruby-mmap"

module Mmap

  # simple caching page manager
  class PageCache
    attr_reader :page_size, :page_path

    def initialize(page_path, options = {})
      # default options
      options = {
        :page_size => 100 * 1024 * 1024,
        :cache_size => 2,
      }.merge(options)

      @cache_size = options.fetch(:cache_size)
      @page_path = page_path
      @page_size = options.fetch(:page_size)
      @cache = {}
      @num_pages = 0
    end

    def page(index)
      @cache[index] || add(index)
    end

    def close
      @cache.values.each{|page| page.close}
    end

    def path(index)
      "#{@page_path}.#{index}"
    end

    private

    def add(index)
      # TBD uncache, LRU?
      @num_pages += 1
      page = Mmap::ByteBuffer.new(path(index), @page_size)
      page.load
      @cache[index] = page
    end
  end

  # single page manager, useful for sized queue where the same
  # page will be used as a ring buffer
  #
  # make sure to use a page size large enough to hold the max number
  # of items + 1 in the queue - FOR NOW there are no checks for overfow
  # this will only be possible once the metadata is refactored into the
  # managers

  class SinglePage
    attr_reader :page_size, :page_path

    def initialize(page_path, options = {})
      # default options
      options = {
        :page_size => 100 * 1024 * 1024,
      }.merge(options)

      @page_path = page_path
      @page_size = options.fetch(:page_size)
      @page = Mmap::ByteBuffer.new(path, @page_size)
    end

    def page(index)
      # single page is always same page, ignore index
      @page
    end

    def close
      @page.close
    end

    def path(index = 0)
      # single page is always page 0, ignore index
      "#{@page_path}.0"
    end
  end

  class MappedMetadata

    # meta structure
    # 00: head page index  (long 8 bytes)
    # 08: head page offset (int  4 bytes)
    # 12: tail page index  (long 8 bytes)
    # 20: tail page offset (int  4 bytes)
    # 24: size             (int  4 byte)

    def initialize(fname)
      existing = File.exists?(fname)
      @meta = Mmap::ByteBuffer.new(fname, 2048) # you know, the constant
      @meta.load
      clear unless existing
    end

    def head_page_index
      @head_page_index ||= @meta.get_long(0)
    end

    def head_page_index=(index)
      @meta.put_long(index, 0)
      @head_page_index = index
    end

    def head_page_offset
      @head_page_offset ||= @meta.get_int(8)
    end

    def head_page_offset=(offset)
      @meta.put_int(offset, 8)
      @head_page_offset = offset
    end

    def tail_page_index
      @tail_page_index ||= @meta.get_long(12)
    end

    def tail_page_index=(index)
      @meta.put_long(index, 12)
      @tail_page_index = index
    end

    def tail_page_offset
      @tail_page_offset ||= @meta.get_int(20)
    end

    def tail_page_offset=(offset)
      @meta.put_int(offset, 20)
      @tail_page_offset = offset
    end

    def size
      @size ||= @meta.get_int(24)
    end
    alias_method :length, :size

    def size=(n)
      @meta.put_int(n, 24)
      @size = n
    end
    alias_method :length=, :size=

    def clear
      self.head_page_index = 0
      self.head_page_offset = 0
      self.tail_page_index = 0
      self.tail_page_offset = 0
      self.size = 0
    end

    def close
      @meta.close
    end
  end

  class PagedQueueError < StandardError; end


  # TBD: refactor the page manager to include the metadata handling and expose
  # interface which abstracts the head/tail pointers

  # PagedQueue
  # non blocking, non thread-safe persistent queue implementation using mmap
  class PagedQueue
    include Enumerable

    attr_reader :meta, :page_usable_size

    INT_BYTES = 4
    LONG_BYTES = 8

    # data structure
    # 0: data size (int  4 bytes)
    # 4: data (data size in bytes)
    # ...
    #
    # a data size of 0 signal the end of the page

    # TBD: add caching/paging class parameter which will allow choosing/defining
    # page management strategies, for example, in a sized queue we may want to reuse
    # pages as in a circular buffer since we know the queue size is limited

    def initialize(page_handler, options = {})
      @page_handler = page_handler
      @page_usable_size = @page_handler.page_size - (2 * INT_BYTES)

      @meta = MappedMetadata.new(@page_handler.page_path)

      # prime the cache with tail & head pages
      @page_handler.page(@meta.head_page_index)
      @page_handler.page(@meta.tail_page_index)
    end

    # @param data [String] write the data string backing bytes to queue
    # @return [Integer] number of data bytes written
    def push(data)
      return 0 unless data.is_a?(String)

      size = data.bytesize
      raise(PagedQueueError, "data size=#{data.bytesize} is larger than usable page size=#{@page_usable_size} (#{@page_handler.page_size} - #{2 * INT_BYTES})") if size > @page_usable_size

      offset = @meta.head_page_offset
      if size > (@page_usable_size - offset)
        # data size is larger than usable data size left, move head to next page
        @meta.head_page_index += 1
        @meta.head_page_offset = offset = 0
      end
      page = @page_handler.page(@meta.head_page_index)
      page.position = offset
      page.put_int(size)
      page.put_bytes(data)

      # write a trailing 0 after the data. on next push, this will be overwritten with next data size
      # since offset will be set on this trailing zero, otherwise if we changed page, trailing zero
      # will be set.
      page.put_int(0)
      @meta.head_page_offset = offset + size + INT_BYTES
      @meta.size += 1

      size
    end
    alias_method :<<, :push

    # skip is like pop but does not read the data, just skip over it
    # @return [Integer] number of data bytes skipped
    def skip
      forward(false)
    end

    # @return [String] retrieve data bytes from queue
    def pop
      forward(true)
    end
    alias_method :shift, :pop

    # iterate & yield over each queue item without pop'ing them
    def each
      offset = @meta.tail_page_offset
      index = @meta.tail_page_index

      while true
        return if index >= @meta.head_page_index && offset >= @meta.head_page_offset

        page = @page_handler.page(index)
        page.position = offset

        if (size = page.get_int) == 0
          # we hit the trailing zero that indicates the end of data on this page
          offset = 0
          index += 1
          return if index >= @meta.head_page_index && offset >= @meta.head_page_offset

          page = @page_handler.page(index)
          page.position = offset
          size = page.get_int
        end

        offset = offset + size + INT_BYTES

        yield page.get_bytes(size)
      end
    end

    def size
      @meta.size
    end
    alias_method :length, :size

    # reset queue to zero without purging any page and without closing
    def clear
      @meta.clear
    end

    # close queue and delete all page files
    def purge
      files = (0..@meta.head_page_index).map{|index| @page_handler.path(index)}
      close
      File.delete(@page_handler.page_path) if File.exist?(@page_handler.page_path)
      files.each{|f| File.delete(f) if File.exist?(f)}
    end

    def close
      @meta.close
      @page_handler.close
    end

    private

    # @param read [Boolean] when true physically read & return the data
    # @return [String|Integer|nil] nil if no items, data String if read is true otherwise size of data
    def forward(read = true)
      offset = @meta.tail_page_offset
      index = @meta.tail_page_index
      return nil if index >= @meta.head_page_index && offset >= @meta.head_page_offset

      page = @page_handler.page(index)
      page.position = offset

      if (size = page.get_int) == 0
        # we hit the trailing zero that indicates the end of data on this page
        offset = @meta.tail_page_offset = 0
        index = @meta.tail_page_index += 1
        return nil if index >= @meta.head_page_index && offset >= @meta.head_page_offset

        page = @page_handler.page(index)
        page.position = offset
        size = page.get_int
      end

      @meta.tail_page_offset = offset + size + INT_BYTES
      @meta.size -= 1

      read ? page.get_bytes(size) : size
    end
  end
end