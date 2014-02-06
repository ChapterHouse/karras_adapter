require 'mongo/document_definition'

class Mongo::DocumentDefinition::Create < Mongo::DocumentDefinition

  private

  def execute
    super.tap { db.create_collection(bound_data['name']) }
  end

end
