require 'mongo/crud'

class Mongo::Crud::Read < Mongo::Crud

  bind_to :query
  options :limit, :fields, :skip

  def initialize(collection_or_name, query={}, options={})
    @transformer = {:transformer => ->(document) { document['id'] = document.delete('_id').to_s.to_i(16) if document.has_key?('_id'); document } }
    super(collection_or_name, query, nil, options)
  end

  private

  def execute
    collection.find(bound_data, options.merge(@transformer))
  end

end
