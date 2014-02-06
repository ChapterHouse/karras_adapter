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


  def execute
    raise NotImplementedError, "Someone forgot to define an execute instance method for class #{self.class.name}"
  end

  def document_definitions
    collection('document_definitions')
  end

end

