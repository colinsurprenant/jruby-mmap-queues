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
      options = {:debug => false, :serialize => true}.merge(options)
      raise(ArgumentError, "queue size must be positive") unless size > 0

      @serialize = options.fetch(:serialize)
      @size = size

      # in-memory queue
      @mq = []
      @num_pop_waiting = 0
      @num_push_waiting = 0
      @mutex = Mutex.new
      @non_empty = ConditionVariable.new
      @non_full = ConditionVariable.new

      # persistent queue
      @pq = PagedQueue.new(fname, 100 * 1024 * 1024)

      # load persistent queue elements into in-memory queue
      @pq.each{|data| push(data, persist = false)}
    end

    def empty?
      @mutex.synchronize{@mq.empty?}
    end

    def push(data, persist = true)
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

        @pq.push(serialize(data)) if persist
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

    def size
      @mq.size
    end
    alias_method :length, :size

    def close
      @pq.close
    end

    def purge
      @pq.purge
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

    # TBD serialization, with pluggable strategy?

    def serialize(data)
      data
    end

    def deserialize(data)
      data
    end
  end
end