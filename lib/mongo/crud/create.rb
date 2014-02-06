require 'mongo/crud'

class Mongo::Crud::Create < Mongo::Crud

  bind_to :document

  def initialize(collection_or_name, document={}, options={})
    super(collection_or_name, nil, document, options)
  end

  private

  def execute
    new_document = bound_data
    collection.insert(new_document, options)
    [{new_document[:id] => new_document[:_id].to_s.to_i(16)}]
  end

end

