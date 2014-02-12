require 'mongo/operation'
require 'mongo/operation/result'

class Mongo::Operation::Base

  include Mongo::Operation

  attr_accessor :fields, :bindings

  def initialize(mongo_source)
    case mongo_source
      when Mongo::Collection
        @collection = mongo_source
      when Mongo::DB
        @db = mongo_source
      when Mongo::Connection
        @connection = mongo_source
      else
        raise "Unusable mongo source #{mongo_source.class}"
    end
  end

  def result(bindings=nil)
    self.bindings = bindings if bindings
    Mongo::Operation::Result.new(execute, fields)
  end

  alias :results :result


  def each(bindings=nil, &block)
    results(bindings).each(&block)
  end

  def columns(bindings=nil)
    results(bindings).columns
  end

  def column_types(bindings=nil)
    results(bindings).column_types
  end

  def to_hash(bindings=nil)
    results(bindings).to_hash
  end

  def map(bindings=nil, &block)
    results(bindings).map(&block)
  end

  alias :map! :map
  alias :collect! :map

  def rows(bindings=nil)
    results(bindings).rows
  end

  def empty?(bindings=nil)
    results(bindings).empty?
  end

  def to_ary(bindings=nil)
    results(bindings).to_ary
  end

  def to_a(bindings=nil)
    results(bindings).to_a
  end

  def [](idx, bindings=nil)
    results(bindings)[idx]
  end

  def last(bindings=nil)
    results(bindings).last
  end


  private


  def execute
    raise NotImplementedError, "Someone forgot to define an execute instance method for class #{self.class.name}"
  end


end

