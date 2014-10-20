require "spec_helper"
require "paged_queue"

KB = 1024
MB = KB ** 2

def purge(base_path)
  Dir["#{base_path}*"].each do |path|
    File.delete(path) if File.exist?(path)
  end
end

managers = [
  {:class => Mmap::PageCache, :options => {:cache_size => 2}},
  {:class => Mmap::SinglePage, :options => {}},
]

managers.each do |manager|
  describe "Mmap::PagedQueue/#{manager[:class].to_s}" do

    before(:all) do
      @path = "spec_mmap_queue_file.dat"
      purge(@path)
      @manager_class = manager[:class]
      @manager_options = manager[:options]
    end

    after(:each) do
      purge(@path)
    end

    it "should work" do
      expect(true).to be true
    end

    it "should create new queue" do
      q = Mmap::PagedQueue.new(@path, MB, @manager_class, @manager_options)

      expect(File.exist?(@path)).to be true
      expect(File.exist?("#{@path}.0")).to be true

      expect(q.meta.head_page_index).to eq(0)
      expect(q.meta.head_page_offset).to eq(0)
      expect(q.meta.tail_page_index).to eq(0)
      expect(q.meta.tail_page_offset).to eq(0)
      expect(q.meta.size).to eq(0)

      q.close
    end

    it "should push/pop" do
      q = Mmap::PagedQueue.new(@path, MB, @manager_class, @manager_options)

      expect(q.push("hello world")).to eq(11)
      expect(q.pop).to eq("hello world")
      expect(q.pop).to be nil
      expect(q.pop).to be nil

      expect(q.push("hello")).to eq(5)
      expect(q.push("world")).to eq(5)
      expect(q.pop).to eq("hello")
      expect(q.pop).to eq("world")
      expect(q.pop).to be nil

      q.close
    end

    it "should skip" do
      q = Mmap::PagedQueue.new(@path, MB, @manager_class, @manager_options)

      expect(q.push("hello")).to eq(5)
      expect(q.push("world")).to eq(5)
      expect(q.skip).to eq(5)
      expect(q.pop).to eq("world")
      expect(q.pop).to be nil

      expect(q.push("foo")).to eq(3)
      expect(q.skip).to eq(3)
      expect(q.pop).to be nil

      q.close
    end

    it "should raise on data bigger than page size" do
      q = Mmap::PagedQueue.new(@path, 10, @manager_class, @manager_options)
      expect{q.push("hello world")}.to raise_error(Mmap::PagedQueueError)
      q.close
    end

    it "should iterate using each" do
      q = Mmap::PagedQueue.new(@path, KB, @manager_class, @manager_options)

      expect(q.push("hello")).to eq(5)
      expect(q.push("world")).to eq(5)
      expect(q.push("foo")).to eq(3)
      expect(q.push("bar")).to eq(3)

      r = []
      q.each{|o| r << o}
      expect(r).to eq(["hello", "world", "foo", "bar"])

      r = []
      q.each{|o| r << o}
      expect(r).to eq(["hello", "world", "foo", "bar"])

      expect(q.pop).to eq("hello")
      r = []
      q.each{|o| r << o}
      expect(r).to eq(["world", "foo", "bar"])

      expect(q.pop).to eq("world")
      r = []
      q.each{|o| r << o}
      expect(r).to eq(["foo", "bar"])

      expect(q.pop).to eq("foo")
      r = []
      q.each{|o| r << o}
      expect(r).to eq(["bar"])

      expect(q.pop).to eq("bar")
      r = []
      q.each{|o| r << o}
      expect(r).to eq([])

      expect(q.push("hello")).to eq(5)
      r = []
      q.each{|o| r << o}
      expect(r).to eq(["hello"])
    end

    it "should be enumerable" do
      q = Mmap::PagedQueue.new(@path, KB, @manager_class, @manager_options)

      expect(q.push("hello")).to eq(5)
      expect(q.push("world")).to eq(5)
      expect(q.push("foo")).to eq(3)
      expect(q.push("bar")).to eq(3)

      expect(q.map{|o| o}).to eq(["hello", "world", "foo", "bar"])
      expect(q.to_a).to eq(["hello", "world", "foo", "bar"])
      expect(q.select{|o| o.size > 3}).to eq(["hello", "world"])
      expect(q.count).to eq(4)
    end

    it "should report size" do
      q = Mmap::PagedQueue.new(@path, KB, @manager_class, @manager_options)
      expect(q.size).to eq(0)

      expect(q.push("hello")).to eq(5)
      expect(q.size).to eq(1)

      expect(q.push("world")).to eq(5)
      expect(q.size).to eq(2)

      expect(q.push("foo")).to eq(3)
      expect(q.size).to eq(3)

      expect(q.push("bar")).to eq(3)
      expect(q.size).to eq(4)

      expect(q.pop).to eq("hello")
      expect(q.size).to eq(3)

      expect(q.skip).to eq(5)
      expect(q.size).to eq(2)

      expect(q.to_a).to eq(["foo", "bar"])
      expect(q.count).to eq(2)

      expect(q.pop).to eq("foo")
      expect(q.size).to eq(1)

      expect(q.pop).to eq("bar")
      expect(q.size).to eq(0)

      expect(q.count).to eq(0)
    end

    it "should clear queue" do
      q = Mmap::PagedQueue.new(@path, KB, @manager_class, @manager_options)

      expect(q.push("foo")).to eq(3)
      q.clear
      expect(q.meta.head_page_index).to eq(0)
      expect(q.meta.head_page_offset).to eq(0)
      expect(q.meta.tail_page_index).to eq(0)
      expect(q.meta.tail_page_offset).to eq(0)
      expect(q.meta.size).to eq(0)

      expect(q.push("bar")).to eq(3)
      expect(q.pop).to eq("bar")

      q.close
    end
  end
end

describe "Mmap::PagedQueue/Mmap::PageCache" do

  before(:all) do
    @path = "spec_mmap_queue_file.dat"
    purge(@path)
    @manager_class = Mmap::PageCache
    @manager_options = {:cache_size => 2}
  end

  after(:each) do
    purge(@path)
  end

  it "should create new pages" do
    q = Mmap::PagedQueue.new(@path, 16, @manager_class, @manager_options)
    expect(q.page_usable_size).to eq(8)

    expect(q.meta.head_page_index).to eq(0)
    expect(q.meta.head_page_offset).to eq(0)
    expect(q.meta.tail_page_index).to eq(0)
    expect(q.meta.tail_page_offset).to eq(0)
    expect(q.meta.size).to eq(0)

    expect(File.exist?(@path)).to be true
    expect(File.exist?("#{@path}.0")).to be true
    expect(File.exist?("#{@path}.1")).to be false

    q.push("hello")
    expect(q.meta.head_page_index).to eq(0)
    expect(File.exist?("#{@path}.1")).to be false

    q.push("world")
    expect(q.meta.head_page_index).to eq(1)
    expect(File.exist?("#{@path}.1")).to be true

    q.push("foobar")
    expect(q.meta.head_page_index).to eq(2)
    expect(File.exist?("#{@path}.2")).to be true

    q.close
  end

  it "should purge page files" do
    q = Mmap::PagedQueue.new(@path, 16, @manager_class, @manager_options)
    expect(q.page_usable_size).to eq(8)

    expect(File.exist?(@path)).to be true
    expect(File.exist?("#{@path}.0")).to be true
    expect(File.exist?("#{@path}.1")).to be false
    expect(File.exist?("#{@path}.2")).to be false

    q.push("hello")
    expect(File.exist?("#{@path}.1")).to be false
    expect(File.exist?("#{@path}.2")).to be false

    q.push("world")
    expect(File.exist?("#{@path}.1")).to be true
    expect(File.exist?("#{@path}.2")).to be false

    q.push("foobar")
    expect(File.exist?("#{@path}.1")).to be true
    expect(File.exist?("#{@path}.2")).to be true

    q.purge
    expect(File.exist?(@path)).to be false
    expect(File.exist?("#{@path}.0")).to be false
    expect(File.exist?("#{@path}.1")).to be false
    expect(File.exist?("#{@path}.2")).to be false
  end
end

describe "Mmap::PagedQueue/Mmap::SinglePage" do

  before(:all) do
    @path = "spec_mmap_queue_file.dat"
    purge(@path)
    @manager_class = Mmap::SinglePage
    @manager_options = {}
  end

  after(:each) do
    purge(@path)
  end

  it "should not create new pages" do
    q = Mmap::PagedQueue.new(@path, 16, @manager_class, @manager_options)
    expect(q.page_usable_size).to eq(8)

    expect(q.meta.head_page_index).to eq(0)
    expect(q.meta.head_page_offset).to eq(0)
    expect(q.meta.tail_page_index).to eq(0)
    expect(q.meta.tail_page_offset).to eq(0)
    expect(q.meta.size).to eq(0)

    expect(File.exist?(@path)).to be true
    expect(File.exist?("#{@path}.0")).to be true
    expect(File.exist?("#{@path}.1")).to be false

    q.push("hello")
    expect(q.meta.head_page_index).to eq(0)
    expect(File.exist?("#{@path}.1")).to be false

    q.push("world")
    expect(q.meta.head_page_index).to eq(1)
    expect(File.exist?("#{@path}.1")).to be false

    q.push("foobar")
    expect(q.meta.head_page_index).to eq(2)
    expect(File.exist?("#{@path}.2")).to be false

    q.close
  end

  it "should purge one page" do
    q = Mmap::PagedQueue.new(@path, 16, @manager_class, @manager_options)
    expect(q.page_usable_size).to eq(8)

    expect(File.exist?(@path)).to be true
    expect(File.exist?("#{@path}.0")).to be true
    expect(File.exist?("#{@path}.1")).to be false
    expect(File.exist?("#{@path}.2")).to be false

    q.push("hello")
    expect(File.exist?("#{@path}.1")).to be false
    expect(File.exist?("#{@path}.2")).to be false

    q.push("world")
    expect(File.exist?("#{@path}.1")).to be false
    expect(File.exist?("#{@path}.2")).to be false

    q.push("foobar")
    expect(File.exist?("#{@path}.1")).to be false
    expect(File.exist?("#{@path}.2")).to be false

    q.purge
    expect(File.exist?(@path)).to be false
    expect(File.exist?("#{@path}.0")).to be false
    expect(File.exist?("#{@path}.1")).to be false
    expect(File.exist?("#{@path}.2")).to be false
  end

  it "should reuse same page as in a ring buffer" do
    # for a 5 1-byte items the queue must be of size (6 x (1 + 4)) + 4
    q = Mmap::PagedQueue.new(@path, 34, @manager_class, @manager_options)
    10.times do
      expect(q.push("a")).to eq(1)
      expect(q.push("b")).to eq(1)
      expect(q.push("c")).to eq(1)
      expect(q.push("d")).to eq(1)
      expect(q.push("e")).to eq(1)

      expect(q.pop).to eq("a")
      expect(q.push("f")).to eq(1)

      expect(q.pop).to eq("b")
      expect(q.push("g")).to eq(1)

      expect(q.pop).to eq("c")
      expect(q.push("h")).to eq(1)

      expect(q.pop).to eq("d")
      expect(q.push("i")).to eq(1)

      expect(q.pop).to eq("e")
      expect(q.push("j")).to eq(1)

      expect(q.pop).to eq("f")
      expect(q.push("k")).to eq(1)

      expect(q.pop).to eq("g")
      expect(q.pop).to eq("h")
      expect(q.pop).to eq("i")
      expect(q.pop).to eq("j")
      expect(q.pop).to eq("k")
      expect(q.pop).to be nil
      expect(q.size).to eq(0)
    end
  end
end
