require "spec_helper"
require "paged_queue"

KB = 1024
MB = KB ** 2

describe Mmap::PagedQueue do
  def purge(base_path)
    Dir["#{base_path}*"].each do |path|
      File.delete(path) if File.exist?(path)
    end
  end

  before(:all) do
    @path = "spec_mmap_queue_file.dat"
    purge(@path)
  end

  after(:each) do
    purge(@path)
  end

  it "should work" do
    expect(true).to be true
  end

  it "should create new queue" do
    q = Mmap::PagedQueue.new(@path, MB)

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
    q = Mmap::PagedQueue.new(@path, MB)

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
    q = Mmap::PagedQueue.new(@path, MB)

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
    q = Mmap::PagedQueue.new(@path, 10)
    expect{q.push("hello world")}.to raise_error(Mmap::PagedQueueError)
    q.close
  end

  it "should create new pages" do
    q = Mmap::PagedQueue.new(@path, 16)
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

  it "should iterate using each" do
    q = Mmap::PagedQueue.new(@path, KB)

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
    q = Mmap::PagedQueue.new(@path, KB)

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
    q = Mmap::PagedQueue.new(@path, KB)
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

end
