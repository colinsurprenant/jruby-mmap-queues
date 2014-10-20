$:.unshift File.join(File.dirname(__FILE__), "/../lib")

require "benchmark"
require "thread"
require "bundler/setup"

require "mapped_sized_queue"

CONSUMERS = 2
PRODUCERS = 1
ITEMS = 500_000
# ITEMS = 500_000 # 5_000_000

Thread.abort_on_exception = true

Benchmark.bmbm(60) do |b|
  b.report("SizedQueue, consumers=#{CONSUMERS}, producers=#{PRODUCERS}}") do
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
  b.report("MappedSizedQueue/PageCache consumers=#{CONSUMERS}, producers=#{PRODUCERS}}") do
    queue = Mmap::MappedSizedQueue.new(
      "cached_mapped_queue_benchmark",
      20,
      :serialize => false,
      :page_size => 20 * 1024 * 1024,
      :manager_class => Mmap::PageCache,
      :manager_options => {:cache_size => 2}
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
  b.report("MappedSizedQueue/SinglePage consumers=#{CONSUMERS}, producers=#{PRODUCERS}}") do
    queue = Mmap::MappedSizedQueue.new(
      "single_mapped_queue_benchmark",
      20,
      :serialize => false,
      :page_size => 2048, # since single page ring-buffer style, size only need contains max queue items
      :manager_class => Mmap::SinglePage,
      :manager_options => {}
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
