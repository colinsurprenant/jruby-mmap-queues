# encoding: utf-8

require "jruby-mmap"

module Mmap

  INT_BYTES = 4
  LONG_BYTES = 8

  # Cursor is a non persisted version of MappedMetadata to support non tail updating reads
  # TODO: should we have a better class model with MappedMetadata?
  class Cursor
    attr_accessor :head_page_index, :head_page_offset, :tail_page_index, :tail_page_offset, :size

    def initialize(meta)
      @head_page_index = meta.head_page_index
      @head_page_offset = meta.head_page_offset

      @tail_page_index = meta.tail_page_index
      @tail_page_offset = meta.tail_page_offset

      @size = meta.size
    end

    alias_method :length, :size
    alias_method :length=, :size=
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

  class PageHandlerError < StandardError; end

  # PageHandler is a base class which implements the basic reading & writing logic and must be extended to
  # minimally implement the mmap_buffer. See PageCache and SinglePage handlers
  class PageHandler
    attr_reader :meta, :page_usable_size

    def initialize(page_path, options = {})
      # default options
      options = {
        :page_size => 100 * 1024 * 1024,
      }.merge(options)

      @page_path = page_path
      @page_size = options.fetch(:page_size)
      @page_usable_size = @page_size - (2 * INT_BYTES)
      @meta = MappedMetadata.new(@page_path)
    end

    # return non queue updating metadata
    # @return [Cursor] non persisting metadata
    def cursor
      Cursor.new(@meta)
    end

    # write data string to queue head
    # @param data [String] data bytes
    # @return [Integer] byte count written
    def write(data)
      return 0 unless data.is_a?(String)

      size = data.bytesize
      return 0 if size == 0

      # TBD verify this overflow calc
      raise(PageHandlerError, "data size=#{data.bytesize} is larger than usable page size=#{@page_usable_size} (#{@page_size} - #{2 * INT_BYTES})") if size > @page_usable_size

      offset = @meta.head_page_offset
      if size > (@page_usable_size - offset)
        # data size is larger than usable data size left, move head to next page
        @meta.head_page_index += 1
        @meta.head_page_offset = offset = 0
      end
      buffer = mmap_buffer(@meta.head_page_index)
      buffer.position = offset
      buffer.put_int(size)
      buffer.put_bytes(data)

      # write a trailing 0 after the data. on next push, this will be overwritten with next data size
      # since offset will be set on this trailing zero, otherwise if we changed page, trailing zero
      # will be set.
      buffer.put_int(0)
      @meta.head_page_offset = offset + size + INT_BYTES
      @meta.size += 1

      size
    end

    # end of queue reached?
    # @param meta [MappedMetadata|Cursor] queue meta information
    # @return [Boolean] true if there are no more elements to read
    def eoq?(meta = @meta)
      meta.tail_page_index >= meta.head_page_index && meta.tail_page_offset >= meta.head_page_offset
    end

    # skip over the next element from the tail or return 0 if none
    # @param meta [MappedMetadata|Cursor] queue meta information
    # @return [Integer] the skipped data size or 0 if none
    def skip(meta = @meta)
      _, size = next_read(meta)
      size
    end

    # read the next element from the tail or return nil if none
    # @param meta [MappedMetadata|Cursor] queue meta information
    # @return [String|Nil] read element data or nil if none
    def read(meta = @meta)
      buffer, size = next_read(meta)
      size == 0 ? nil : buffer.get_bytes(size)
    end

    def mmap_buffer(index)
      raise("abstract method")
    end

    # @param index [Integer] page index
    # @return [String] path to page file for given page index
    def page_index_path(index)
      "#{@page_path}.#{index}"
    end

    # @return [Enumerator<Integer>] list of all active mmap pages index from tail to head
    def page_indexes
      @meta.tail_page_index..@meta.head_page_index
    end

    def purge_page_index(index)
      f = page_index_path(index)
      File.delete(f) if File.exist?(f)
    end

    # called with the the tail page index before incrementing
    # override to change behaviour
    def purge_unused_page_index(index)
      purge_page_index(index)
    end

    def purge
      close
      File.delete(@page_path) if File.exist?(@page_path)
      page_indexes.each{|i| purge_page_index(i)}
    end

    # if overriding, my sure to call super() so that @meta et closed
    def close
      @meta.close
    end

    private

    EOQ = [nil, 0].freeze

    # retrieve the page buffer set for the next read position
    # @param meta [MappedMetadata|Cursor] queue meta information, pass a Cursor for non queue updating reads
    # @return 2-tuple [Mmap::ByteBuffer, Integer] with the buffer positioned at the next read location and the next read size
    def next_read(meta)
      offset = meta.tail_page_offset
      index = meta.tail_page_index
      return EOQ if index >= meta.head_page_index && offset >= meta.head_page_offset

      buffer = mmap_buffer(index)
      buffer.position = offset

      if (size = buffer.get_int) == 0
        # we hit the trailing zero that indicates the end of data on this page
        # this tail page is now unsused and we will move to the next one
        offset = meta.tail_page_offset = 0
        purge_unused_page_index(meta.tail_page_index)
        index = meta.tail_page_index += 1
        return EOQ if index >= meta.head_page_index && offset >= meta.head_page_offset

        buffer = mmap_buffer(index)
        buffer.position = offset
        size = buffer.get_int
      end

      meta.tail_page_offset = offset + size + INT_BYTES
      meta.size -= 1

      [buffer, size]
    end

  end

  # simple caching page handler
  class PageCache < PageHandler
    attr_reader :page_size, :page_path

    def initialize(page_path, options = {})
      super

      # default options
      options = {
        :cache_size => 2,
      }.merge(options)

      @cache_size = options.fetch(:cache_size)
      @cache = {}
      @lru = []

      # prime the cache with tail & head pages
      mmap_buffer(@meta.head_page_index)
      mmap_buffer(@meta.tail_page_index)
    end

    def mmap_buffer(index)
      @cache[index] || add(index)
    end

    def close
      super()
      @cache.values.each{|buffer| buffer.close}
      @cache.clear
      @lru.clear
    end

    private

    # @param index [Integer] the page index to cache
    # @return [ByteBuffer] newly cached ByteBuffer
    def add(index)
      if @lru.size >= @cache_size
        buffer = @cache.delete(@lru.shift)
        buffer.close
      end
      buffer = Mmap::ByteBuffer.new(page_index_path(index), @page_size)
      buffer.load
      @lru << index
      @cache[index] = buffer
    end
  end

  # single page manager, useful for sized queue where the same
  # page will be used as a ring buffer
  #
  # make sure to use a page size large enough to hold the max number
  # of items + 1 in the queue - FOR NOW there are no checks for overfow
  # this will only be possible once the metadata is refactored into the
  # managers

  class SinglePage < PageHandler
    attr_reader :page_size, :page_path

    def initialize(page_path, options = {})
      super
      @buffer = Mmap::ByteBuffer.new(page_index_path(0), @page_size)
      @buffer.load
    end

    def mmap_buffer(index)
      # always same buffer, ignore index
      @buffer
    end

    def purge_unused_page_index(index)
      # do not purge, tail page index will be incremented but
      # will always endup mapping to same @buffer so there is
      # never a unused page.
    end

    def close
      super()
      @buffer.close
    end

    def page_index_path(index = 0)
      # always page 0, ignore index
      "#{@page_path}.0"
    end

    def page_indexes
      # always only page 0
      [page_index_path]
    end
  end

  # PagedQueue
  # non blocking, non thread-safe persistent queue implementation using mmap
  class PagedQueue
    include Enumerable

    # data structure
    # 0: data size (int  4 bytes)
    # 4: data (data size in bytes)
    # ...
    #
    # a data size of 0 signal the end of the page

    def initialize(page_handler, options = {})
      @page_handler = page_handler
      @closed = false
    end

    # @param data [String] write the data string backing bytes to queue
    # @return [Integer] number of data bytes written
    def push(data)
      @page_handler.write(data)
    end
    alias_method :<<, :push

    # skip is like pop but does not read the data, just skip over it
    # @return [Integer] number of data bytes skipped
    def skip
      @page_handler.skip
    end

    # @return [String] retrieve data bytes from queue
    def pop
      @page_handler.read
    end
    alias_method :shift, :pop

    # iterate & yield over each queue item without pop'ing them
    def each
      meta = @page_handler.cursor
      while true
        data = @page_handler.read(meta)
        return unless data
        yield(data)
      end
    end

    def size
      @page_handler.meta.size
    end
    alias_method :length, :size

    # reset queue to zero without purging any page and without closing
    def clear
      @page_handler.meta.clear
    end

    # close queue and delete all page files
    def purge
      close
      @page_handler.purge
    end

    def close
      @page_handler.close unless @closed
      @closed = true
    end
  end
end