require "spec_helper"

def purge(base_path)
  Dir["#{base_path}*"].each do |path|
    File.delete(path) if File.exist?(path)
  end
end

handlers = [
  {:class => Mmap::PageCache, :options => {:page_size => 1024, :cache_size => 2}},
  {:class => Mmap::SinglePage, :options => {:page_size => 1024}},
]

handlers.each do |handler|
  describe "Mmap::SizedQueue/#{handler[:class].to_s}" do

    before(:all) do
      @path = "spec_sized_queue_file.dat"
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
      q = Mmap::SizedQueue.new(@path, 10, :page_handler => handler[:class].new(@path, handler[:options]))
      expect(q.size).to eq(0)
      expect(q.empty?).to be true
      q.close
    end

    it "should push and pop" do
      q = Mmap::SizedQueue.new(@path, 10, :page_handler => handler[:class].new(@path, handler[:options]))
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
      q = Mmap::SizedQueue.new(@path, 10, :page_handler => handler[:class].new(@path, handler[:options]))
      expect(q.size).to eq(0)
      expect(q.empty?).to be true
      q.push("foo")
      q.push("bar")
      expect(q.size).to eq(2)
      expect(q.empty?).to be false
      q.close

      q = Mmap::SizedQueue.new(@path, 10, :page_handler => handler[:class].new(@path, handler[:options]))
      expect(q.size).to eq(2)
      expect(q.empty?).to be false

      expect(q.pop).to eq("foo")
      expect(q.pop).to eq("bar")
      expect(q.size).to eq(0)
      expect(q.empty?).to be true
      q.close

      q = Mmap::SizedQueue.new(@path, 10, :page_handler => handler[:class].new(@path, handler[:options]))
      expect(q.size).to eq(0)
      expect(q.empty?).to be true
      q.close
    end
  end
end
