$:.unshift File.join(File.dirname(__FILE__), "/../lib")

require "benchmark"
require "thread"
require "bundler/setup"

require "jruby-mmap-queues"

CONSUMERS = 2
PRODUCERS = 1
ITEMS = 500_000
# ITEMS = 500_000 # 5_000_000

Thread.abort_on_exception = true

Benchmark.bmbm(60) do |b|
  b.report("SizedQueue, consumers=#{CONSUMERS}, producers=#{PRODUCERS}") do
    queue = SizedQueue.new(20)

    consumers = CONSUMERS.times.map do
      Thread.new do
        while true
          data = queue.pop
          break if data == "END"
        end
      end
    end

    producers = PRODUCERS.times.map do
      Thread.new do
        ITEMS.times.each{|i| queue << "somedata #" + i.to_s}
      end
    end

    producers.each(&:join)
    consumers.each{queue << "END"}
    consumers.each(&:join)
  end
end

Benchmark.bmbm(60) do |b|
  b.report("Mmap::SizedQueue/PageCache consumers=#{CONSUMERS}, producers=#{PRODUCERS}") do
    queue = Mmap::SizedQueue.new("cached_mapped_queue_benchmark", 20,
      :page_handler => Mmap::PageCache.new("cached_mapped_queue_benchmark", :page_size => 20 * 1024 * 1024, :cache_size => 2)
    )
    queue.clear
    raise unless queue.empty?

    consumers = CONSUMERS.times.map do
      Thread.new do
        while true
          data = queue.pop
          break if data == "END"
        end
      end
    end

    producers = PRODUCERS.times.map do
      Thread.new do
        ITEMS.times.each{|i| queue << "somedata #" + i.to_s}
      end
    end

    producers.each(&:join)
    consumers.each{queue << "END"}
    consumers.each(&:join)

    queue.close
  end
end


Benchmark.bmbm(60) do |b|
  b.report("Mmap::SizedQueue/SinglePage consumers=#{CONSUMERS}, producers=#{PRODUCERS}") do
    queue = Mmap::SizedQueue.new("single_mapped_queue_benchmark", 20,
      :page_handler => Mmap::SinglePage.new("single_mapped_queue_benchmark", :page_size => 2048)
    )
    queue.clear
    raise unless queue.empty?

    consumers = CONSUMERS.times.map do
      Thread.new do
        while true
          data = queue.pop
          break if data == "END"
        end
      end
    end

    producers = PRODUCERS.times.map do
      Thread.new do
        ITEMS.times.each{|i| queue << "somedata #" + i.to_s}
      end
    end

    producers.each(&:join)
    consumers.each{queue << "END"}
    consumers.each(&:join)

    queue.close
  end
end
