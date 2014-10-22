# encoding: utf-8

require "thread"
require "jruby-mmap-queues/paged_queue"

module Mmap
  class NoSerializer
    def serialize(data); data end
    def deserialize(data); data end
  end

  # SizedQueue blocking thread-safe sized queue
  # uses both an in-memory queue and a persistent queue and pushes to both
  # but pops from the in-memory queue.
  class SizedQueue

    # @param path [String] the queue base file name
    # @param size [Integer] the queue max size
    # @param options [Hash]
    # @option options [Boolean] :debug, default to false
    # @option options [Boolean] :seralize, serialize to json, default to true
    def initialize(path, size, options = {})

      # default options
      options = {
        :debug => false,
        :page_handler => Mmap::PageCache.new(path),
        :serializer => Mmap::NoSerializer.new,
      }.merge(options)

      raise(ArgumentError, "queue size must be positive") unless size > 0

      @serializer = options.fetch(:serializer)

      @size = size

      # in-memory queue
      @mq = []
      @mutex = Mutex.new
      @non_empty = ConditionVariable.new
      @non_full = ConditionVariable.new

      # persistent queue
      @pq = PagedQueue.new(options[:page_handler])

      # load existing persistent queue elements into in-memory queue
      @pq.each{|data| push(@serializer.deserialize(data), persist = false)}
    end

    def empty?
      @mutex.lock
      begin
        return @mq.empty?
      ensure
        @mutex.unlock #rescue nil
      end
    end

    def push(data, persist = true)
      @mutex.lock
      begin
        while true
          if @mq.size < @size
            @pq.push(@serializer.serialize(data)) if persist
            @mq.push(data)

            @non_empty.signal
            return self
          end

          @non_full.wait(@mutex)
        end
      ensure
        @mutex.unlock #rescue nil
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
            return data #in-memory object, no need to deserialize
          end
          raise(ThreadError, "queue empty") if non_block

          @non_empty.wait(@mutex)
        end
      ensure
        @mutex.unlock #rescue nil
      end
    end
    alias_method :shift, :pop

    def size
      @mutex.lock
      begin
        return @mq.size
      ensure
        @mutex.unlock #rescue nil
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
      clear
      @pq.purge
    end

  end
end