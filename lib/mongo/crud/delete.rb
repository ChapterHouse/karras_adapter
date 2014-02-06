require 'mongo/crud'

class Mongo::Crud::Delete  < Mongo::Crud

  bind_to :query

  def initialize(collection_or_name, query={}, options={})
    super(collection_or_name, query, nil, options)
  end

  def execute
    collection.remove(bound_data, options)
  end

end
