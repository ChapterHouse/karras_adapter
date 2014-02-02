require 'mongo/operation'

module Mongo

  class Finder < Mongo::Operation::Base

    attr_accessor :query, :options

    def initialize(collection_or_name, query={}, options={})
      collection_or_name = db.collection(collection_or_name.to_s) unless collection_or_name.is_a?(Mongo::Collection)
      @query = query
      @options = options
      @transformer = {:transformer => ->(document) { document['id'] = document.delete('_id').to_s.to_i(16) if document.has_key?('_id'); document } }
      super(collection_or_name)
    end

    def limit
      options[:limit]
    end

    def limit=(new_limit)
      options[:limit] = new_limit
    end

    def fields
      options[:fields]
    end

    def fields=(new_fields)
      options[:fields] = new_fields
    end

    def skip
      options[:skip]
    end

    def skip=(new_skip)
      options[:skip] = new_skip
    end


    private

    attr_reader :transformer

    def execute
      kwery = query.dup
      # TODO: Abort if not enough bindings are given
      bindings.each do |binding|
        field = binding.first
        value = binding.last
        field = field.name if field.is_a?(ActiveRecord::ConnectionAdapters::Column)
        kwery[field] = value if kwery.has_key?(field)
      end if bindings
      kwery['_id'] = BSON::ObjectId.from_string(kwery.delete('id').to_i.to_s(16)) if kwery.has_key?('id')
      collection.find(kwery, options.merge(transformer))
    end

  end


end
