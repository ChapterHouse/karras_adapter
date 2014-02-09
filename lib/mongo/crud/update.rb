require 'mongo/crud'

class Mongo::Crud::Update < Mongo::Crud

  bind_to :document

  def execute
    kwery = query.dup
    kwery['_id'] = BSON::ObjectId.from_string(kwery.delete('id').to_i.to_s(16)) if kwery.has_key?('id')
    collection.update(kwery, {"$set" =>  bound_data }, options)
    Mongo::Crud::Read.new(collection, kwery).results
  end

end
