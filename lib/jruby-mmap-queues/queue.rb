# encoding: utf-8

require "thread"
require "jruby-mmap-queues/paged_queue"

module Mmap

  # TODO: refactor/DRY with sized_queue.rb
  unless defined?(NoSerializer)
    class NoSerializer
      def serialize(data); data; end
      def deserialize(data); data; end
    end
    NO_SERIALIZER = NoSerializer.new
  end

  # Queue blocking thread-safe queue
  # uses both an in-memory queue and a persistent queue and pushes to both
  # but pops from the in-memory queue.
  class Queue

    # @param options [Hash]
    # @option options [PageHandler] :page_handler, page handler object, defaults to Mmap::PageCache
    # @option options [String] :path, queue file path when not specifying a :page_handler. defaults to "sized_queue.dat"
    # @option options [Boolean] :debug, default to false
    # @option options [Object] :seralizer, persistence serialization object, defaults to NoSerializer
    def initialize(options = {})
      # default options
      options = {
        :path => "queue.dat", # default path when no page handler is specified
        :debug => false,
        :serializer => NO_SERIALIZER,
      }.merge(options)

      @serializer = options.fetch(:serializer)

      @mutex = Mutex.new
      @non_empty = ConditionVariable.new

      # persistent queue, use :page_handler option or by default Mmap::PageCache + :path option
      @pq = PagedQueue.new(options[:page_handler] || Mmap::PageCache.new(options[:path]))
    end

    def empty?
      @mutex.lock
      begin
        return @pq.size <= 0
      ensure
        @mutex.unlock #rescue nil
      end
    end

    def push(data)
      @mutex.lock
      begin
        @pq.push(@serializer.serialize(data))

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
          return @serializer.deserialize(@pq.pop) if @pq.size > 0
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
        return @pq.size
      ensure
        @mutex.unlock #rescue nil
      end
    end
    alias_method :length, :size

    def clear
      @mutex.synchronize do
        @pq.clear
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