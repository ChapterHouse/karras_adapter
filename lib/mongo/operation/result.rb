require 'mongo/operation'
require 'active_record/result'
require 'set'

class Mongo::Operation::Result < ActiveRecord::Result

  include Enumerable

  attr_reader :results

  def initialize(mongo_operation_base_result, fields=nil)
    @results = mongo_operation_base_result
    @fields = fields.is_a?(Set) ? fields.dup : fields.to_set if fields
  end

  def each
    results.each { |result| yield result }
  end

  def columns
    # TODO: Should we only return the keys that are present in all results? I dunno. Thats what is making this Mongo adapter so interesting.
    # We call it fields until we cross the Mongo/ActiveRecord boundry.
    if @fields.nil?
      @fields = Set.new
      each { |result| @fields += result.keys }
    end
    @fields
  end

  def column_types
    []
  end

  def to_hash
    results
  end

  alias :map! :map
  alias :collect! :map

  def rows
    @rows ||= results.map(&:values)
  end

  def empty?
    results.empty?
  end

  def to_ary
    results
  end

  def [](idx)
    results.last
  end

  def last
    results.last
  end

  def initialize_copy(other)
    @results = other.results.dup
  end

end
