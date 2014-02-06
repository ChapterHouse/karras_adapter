require 'mongo/operation/base'

class Mongo::Crud < Mongo::Operation::Base

  attr_accessor :query, :document, :options

  def self.options(*names)
    names.each do |name|
      name = name.to_s
      option = name.to_sym
      class_eval <<-READER, __FILE__, __LINE__ + 1
            def #{name}
              options[#{option.inspect}]
            end
      READER

      class_eval <<-WRITER, __FILE__, __LINE__ + 1
            def #{name}=(new_value)
              options[#{option.inspect}] = new_value
            end
      WRITER
    end
  end

  def self.bind_to(target=nil)
    if target
      @target = target.to_sym
    elsif @target.nil? && superclass < Mongo::Operation::Base
      superclass.bind_to
    else
      @target
    end
  end

  def initialize(collection_or_name, query={}, document={}, options={})
    collection_or_name = db.collection(collection_or_name.to_s) unless collection_or_name.is_a?(Mongo::Collection)
    @query = query
    @document = document
    @options = options
    super(collection_or_name)
  end

  private

  def bound_data
    case self.class.bind_to
      when :query
        bind query
      when :document
        bind document
      when NilClass
        raise "Binding destination not specified. Please use 'bind_to :query' or 'bind_to :document' in the definition of #{self.class.name}"
      else
        raise "Unknown binding destination #{self.class.bind_to.inspect}. Please use 'bind_to :query' or 'bind_to :document' in the definition of #{self.class.name}"
    end
  end

  def bind(hash)
    hash = hash.dup
    # TODO: Abort if not enough bindings are given
    bindings.each do |binding|
      field = binding.first
      value = binding.last
      field = field.name if field.is_a?(ActiveRecord::ConnectionAdapters::Column)
      hash[field] = value if hash.has_key?(field)
    end if bindings
    hash['_id'] = BSON::ObjectId.from_string(hash.delete('id').to_i.to_s(16)) if hash.has_key?('id')
    hash
  end

end

require 'mongo/crud/create'
require 'mongo/crud/read'
require 'mongo/crud/update'
require 'mongo/crud/delete'