require 'mongo/operation'

module Mongo

  class Remover < Mongo::Operation::Base

    attr_accessor :query, :options

    def initialize(collection_or_name, query={}, options={})
      collection_or_name = db.collection(collection_or_name.to_s) unless collection_or_name.is_a?(Mongo::Collection)
      @query = query
      @options = options
      super(collection_or_name)
    end

    def execute
      kwery = query.dup
      # TODO: Abort if not enough bindings are given (don't forget some values might come in not needing binds.)
      bindings.each do |binding|
        field = binding.first
        value = binding.last
        field = field.name if field.is_a?(ActiveRecord::ConnectionAdapters::Column)
        kwery[field] = value if kwery.has_key?(field)
      end if bindings
      kwery['_id'] = BSON::ObjectId.from_string(kwery.delete('id').to_i.to_s(16)) if kwery.has_key?('id')
      collection.remove(kwery, options)
    end


  end

end
