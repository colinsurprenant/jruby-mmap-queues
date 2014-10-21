# encoding: utf-8

require "thread"
require "paged_queue"

module Mmap

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

      # default options
      options = {
        :debug => false,
        :serialize => true,
        :page_size => 100 * 1024 * 1024,
        :manager_class => Mmap::PageCache,
        :manager_options => {:cache_size => 2},
      }.merge(options)

      raise(ArgumentError, "queue size must be positive") unless size > 0

      @serialize = options.fetch(:serialize)
      @size = size

      # in-memory queue
      @mq = []
      @mutex = Mutex.new
      @non_empty = ConditionVariable.new
      @non_full = ConditionVariable.new

      # persistent queue
      @pq = PagedQueue.new(fname, options[:page_size], options[:manager_class], options[:manager_options])

      # load existing persistent queue elements into in-memory queue
      @pq.each{|data| push(data, persist = false)}
    end

    def empty?
      @mutex.synchronize{@mq.empty?}
    end

    def push(data, persist = true)
      @mutex.lock
      begin
        while true
          break if @mq.length < @size
          @non_full.wait(@mutex)
        end

        @pq.push(serialize(data)) if persist
        @mq.push(data)

        @non_empty.signal
      ensure
        @mutex.unlock rescue nil
      end
    end
    alias_method :<<, :push

    def pop(non_block = false)
      @mutex.lock
      begin
        while true
          unless @mq.empty?
            @pq.skip
            data = @mq.shift
            @non_full.signal if @mq.length < @size
            return data
          end
          raise(ThreadError, "queue empty") if non_block

          @non_empty.wait(@mutex)
        end
      ensure
        @mutex.unlock rescue nil
      end
    end
    alias_method :shift, :pop

    def size
      @mutex.lock
      begin
        return @mq.size
      ensure
        @mutex.unlock rescue nil
      end
    end
    alias_method :length, :size

    def clear
      @mutex.synchronize do
        @mq = []
        @non_full.signal
        @pq.clear
      end
    end

    def close
      @pq.close
    end

    def purge
      @pq.purge
    end

    private

    # TBD serialization, with pluggable strategy?

    def serialize(data)
      data
    end

    def deserialize(data)
      data
    end
  end
end