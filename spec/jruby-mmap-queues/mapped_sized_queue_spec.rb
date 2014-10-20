require "spec_helper"
require "mapped_sized_queue"

def purge(base_path)
  Dir["#{base_path}*"].each do |path|
    File.delete(path) if File.exist?(path)
  end
end

managers = [
  {:manager_class => Mmap::PageCache, :manager_options => {:cache_size => 2}},
  {:manager_class => Mmap::SinglePage, :manager_options => {}},
]

managers.each do |manager|
  describe "Mmap::MappedSizedQueue/#{manager[:class].to_s}" do

    before(:all) do
      @path = "spec_mapped_sized_queue_file.dat"
      purge(@path)
    end

    after(:all) do
    end

    after(:each) do
      purge(@path)
    end

    it "should work" do
      expect(true).to eq(true)
    end

    it "should create new empty queue" do
      q = Mmap::MappedSizedQueue.new(@path, 10, manager)
      expect(q.size).to eq(0)
      expect(q.empty?).to be true
      q.close
    end

    it "should push and pop" do
      q = Mmap::MappedSizedQueue.new(@path, 10, manager)
      q.push("foo")
      q.push("bar")
      expect(q.size).to eq(2)
      expect(q.empty?).to be false

      expect(q.pop).to eq("foo")
      expect(q.pop).to eq("bar")
      expect(q.size).to eq(0)
      expect(q.empty?).to be true

      q.close
    end

    it "should reload existing persistent queue" do
      q = Mmap::MappedSizedQueue.new(@path, 10, manager)
      expect(q.size).to eq(0)
      expect(q.empty?).to be true
      q.push("foo")
      q.push("bar")
      expect(q.size).to eq(2)
      expect(q.empty?).to be false
      q.close

      q = Mmap::MappedSizedQueue.new(@path, 10, manager)
      expect(q.size).to eq(2)
      expect(q.empty?).to be false

      expect(q.pop).to eq("foo")
      expect(q.pop).to eq("bar")
      expect(q.size).to eq(0)
      expect(q.empty?).to be true
      q.close

      q = Mmap::MappedSizedQueue.new(@path, 10, manager)
      expect(q.size).to eq(0)
      expect(q.empty?).to be true
      q.close
    end
  end
end
