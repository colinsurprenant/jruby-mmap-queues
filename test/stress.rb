$:.unshift File.expand_path("../../lib", __FILE__)

require "thread"
require "bundler/setup"
require "mapped_sized_queue"

Thread.abort_on_exception = true

ITEMS = 500_000
SOURCE = ITEMS.times.map{|i| "somedata #" + ("%07d" % i)}

[[1, 4], [4, 1], [4, 4]].each do |consumers_count, producers_count|

  expected_result = (SOURCE * producers_count).sort

  puts("consumers=#{consumers_count}, producers=#{producers_count}")

  definitions = [
    {
      :name => "MappedSizedQueue/PageCache",
      :queue => Mmap::MappedSizedQueue.new(
        "cached_mapped_queue_benchmark",
        20,
        :serialize => false,
        :page_size => 20 * 1024 * 1024,
        :manager_class => Mmap::PageCache,
        :manager_options => {:cache_size => 2}
      )
    },
    {
      :name => "MappedSizedQueue/SinglePage",
      :queue => Mmap::MappedSizedQueue.new(
        "single_mapped_queue_benchmark",
        20,
        :serialize => false,
        :page_size => 2048, # since single page ring-buffer style, size only need contains max queue items
        :manager_class => Mmap::SinglePage,
        :manager_options => {}
      )
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
    queue.close
  end
end
