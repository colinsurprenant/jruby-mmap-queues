# encoding: utf-8

require "thread"
require "jruby-mmap-queues/paged_queue"

module Mmap
  class NoSerializer
    def serialize(data); data; end
    def deserialize(data); data; end
  end
  NO_SERIALIZER = NoSerializer.new

  # SizedQueue blocking thread-safe sized queue
  # uses both an in-memory queue and a persistent queue and pushes to both
  # but pops from the in-memory queue.
  class SizedQueue

    # @param size [Integer] the queue max size
    # @param options [Hash]
    # @option options [PageHandler] :page_handler, page handler object, defaults to Mmap::PageCache
    # @option options [String] :path, queue file path when not specifying a :page_handler. defaults to "sized_queue.dat"
    # @option options [Boolean] :debug, default to false
    # @option options [Object] :seralizer, persistence serialization object, defaults to NoSerializer
    def initialize(size, options = {})
      # default options
      options = {
        :path => "sized_queue.dat", # default path when no page handler is specified
        :debug => false,
        :serializer => NO_SERIALIZER,
      }.merge(options)

      raise(ArgumentError, "queue size must be positive") unless size > 0

      @serializer = options.fetch(:serializer)

      @size = size

      # in-memory queue
      @mq = []
      @mutex = Mutex.new
      @non_empty = ConditionVariable.new
      @non_full = ConditionVariable.new

      # persistent queue, use :page_handler option or by default Mmap::PageCache + :path option
      @pq = PagedQueue.new(options[:page_handler] || Mmap::PageCache.new(options[:path]))

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
        # must always verify actual condition upon waking up from condition variable
        # since it is possible for thread to wake up for other reasons
        @non_full.wait(@mutex) while @mq.size >= @size
        @pq.push(@serializer.serialize(data)) if persist
        @mq.push(data)
        @non_empty.signal

        return self
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

          # must always verify actual condition upon waking up from condition variable
          # since it is possible for thread to wake up for other reasons
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
        @pq.clear
        @non_full.signal
      end
    end

    def close
      @pq.close
    end

    def purge
      @pq.purge
    end

  end
end