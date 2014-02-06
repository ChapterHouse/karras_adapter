require 'mongo/crud'

class Mongo::DocumentDefinition < Delegator

  def initialize(name, fields)
    superklass = Object.const_get(self.class.name.sub('DocumentDefinition', 'Crud'))
    super superklass.new('document_definitions', { 'name' => name, 'fields' => fields })
  end

  def __getobj__
    @delegate
  end

  def __setobj__(obj)
    @delegate = obj
  end

end

require 'mongo/document_definition/create'
require 'mongo/document_definition/read'
require 'mongo/document_definition/update'
require 'mongo/document_definition/delete'



