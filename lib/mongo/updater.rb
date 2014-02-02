require 'mongo/operation'
require 'mongo/finder'

module Mongo

  class Updater < Mongo::Operation::Base

    attr_accessor :document, :query, :options

    def initialize(collection_or_name, query={}, document={}, options={})
      collection_or_name = db.collection(collection_or_name.to_s) unless collection_or_name.is_a?(Mongo::Collection)
      @query = query
      @document = document
      @options = options
      super(collection_or_name)
    end

    def execute
      kwery = query.dup
      kwery['_id'] = BSON::ObjectId.from_string(kwery.delete('id').to_i.to_s(16)) if kwery.has_key?('id')

      doc = document.dup
      # TODO: Abort if not enough bindings are given (don't forget some values might come in not needing binds.)
      bindings.each do |binding|
        field = binding.first
        value = binding.last
        field = field.name if field.is_a?(ActiveRecord::ConnectionAdapters::Column)
        doc[field] = value if doc.has_key?(field)
      end if bindings
      doc['_id'] = BSON::ObjectId.from_string(doc.delete('id').to_i.to_s(16)) if doc.has_key?('id')
      collection.update(kwery, {"$set" =>  doc }, options)
      Mongo::Finder.new(collection, query)
    end


  end

end
