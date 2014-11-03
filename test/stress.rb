$:.unshift File.expand_path("../../lib", __FILE__)

require "thread"
require "bundler/setup"
require "jruby-mmap-queues"

Thread.abort_on_exception = true

ITEMS = 1_000_000
SOURCE = ITEMS.times.map{|i| "somedata #" + ("%07d" % i)}
PAGE_SIZE = 1 * 1024 * 1024

[[1, 4], [4, 1], [4, 4]].each do |consumers_count, producers_count|

  expected_result = (SOURCE * producers_count).sort

  puts("consumers=#{consumers_count}, producers=#{producers_count}")

  definitions = [
    {
      :name => "SizedQueue/PageCache",
      :queue => Mmap::SizedQueue.new(20, :page_handler => Mmap::PageCache.new("cached_mapped_queue_benchmark", :page_size => PAGE_SIZE, :cache_size => 2))
    },
    {
      :name => "SizedQueue/SinglePage",
      :queue => Mmap::SizedQueue.new(20, :page_handler => Mmap::SinglePage.new("single_mapped_queue_benchmark", :page_size => PAGE_SIZE))
    }
  ]

  definitions.each do |definition|
    print("  #{definition[:name]}...")
    queue = definition[:queue]
    queue.clear
    all_results = Queue.new

    consumers = consumers_count.times.map do
      Thread.new do
        consumer_results = []
        while true
          data = queue.pop
          break if data == "END"
          consumer_results << data
        end
        all_results << consumer_results
      end
    end

    producers = producers_count.times.map do
      Thread.new do
        SOURCE.each{|data| queue << data}
      end
    end

    producers.each(&:join)
    consumers.each{queue << "END"}
    consumers.each(&:join)

    results = []
    consumers.each {results = results + all_results.pop}
    raise if results.sort != expected_result
    puts(" success")
    queue.purge
  end
end
