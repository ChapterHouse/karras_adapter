require 'mongo/operation'

module Mongo

  class Inserter < Mongo::Operation::Base

    attr_accessor :document, :options

    def initialize(collection_or_name, document={}, options={})
      collection_or_name = db.collection(collection_or_name.to_s) unless collection_or_name.is_a?(Mongo::Collection)
      @document = document
      @options = options
      super(collection_or_name)
    end

    private

    def execute
      doc = document.dup
      # TODO: Abort if not enough bindings are given
      # TODO: Handle embedded documents
      bindings.each do |binding|
        field = binding.first
        value = binding.last
        field = field.name if field.is_a?(ActiveRecord::ConnectionAdapters::Column)
        doc[field] = value if doc.has_key?(field)
      end if bindings
      collection.insert(doc, options)
      [{doc[:id] => doc[:_id].to_s.to_i(16)}]
      #keys = doc.keys
      #[{doc[:_id] => keys.inject([doc[:_id].to_s.to_i(16)]) { |values, key| values << doc[key] } }]
    end

  end


end
