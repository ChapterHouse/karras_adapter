require 'mongo/inserter'


module Mongo

  class Definer < Mongo::Inserter

    def initialize(name, fields)
      super('document_definitions', { 'name' => name, 'fields' => fields })
    end

    private

    def execute
      super.tap { db.create_collection(bound_data['name']) }
    end

  end


end