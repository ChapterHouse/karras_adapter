require 'mongo/crud'

class Mongo::DocumentDefinition < Delegator

  # TODO: Something quasi dynamic when it's not in the document definitions?
  def self.for(name)
    Mongo::DocumentDefinition::Read.new(name).results.first
    #document_definitions.find_one( { 'name' => name } )
    #collection('document_definitions').find_one( { name => { '$exists' => true } } )
  end

  def self.fields_for(name)
    self.for(name)['fields']
  end

  def field_names_for(name)
    fields_for(name).keys
  end

  def initialize(name, fields=nil)
    #Mongo::Crud::Read.new('document_definitions', {'name' => 'schema_migrations'}).results.to_a
    #Mongo::DocumentDefinition::Read.new('schema_migrations').results.to_a
    superklass = Object.const_get(self.class.name.sub('DocumentDefinition', 'Crud'))
    # TODO: Better variable name
    hash = { 'name' => name }
    hash['fields'] = fields if fields
    super superklass.new('document_definitions', hash)
    initialize_document_definitions
  end

  def __getobj__
    @delegate
  end

  def __setobj__(obj)
    @delegate = obj
  end

  private


  # TODO: Cahe the need for this at the class level so it is only done once.
  def initialize_document_definitions
    unless db.collection_names.include?('document_definitions')
      # TODO: Work out a class recusive way to habdle one or both of these statements. Try and keep all of the direct collection(name) statements inside of the crud proper.
      collection('document_definitions').insert(
          'name' => 'document_definitions',
          'fields' => {
              'name' =>   { 'type' => :string, 'limit' => nil, 'precision' => nil, 'scale' => nil, 'default' => nil, 'null' => false, 'first' => nil, 'after' => nil, 'primary_key' => true},
              # TODO: Umm type idunno? Return to this when you get AR to deal with flux types.
              'fields' => { 'type' => :string,   'limit' => nil, 'precision' => nil, 'scale' => nil, 'default' => nil, 'null' => false, 'first' => nil, 'after' => nil, 'primary_key' => nil}
          }
      )

      collection('document_definitions').insert(
          'name' => 'system.indexes',
          'fields' => {
              'v'    => { 'type' => :integer, 'limit' => nil, 'precision' => nil, 'scale' => nil, 'default' => nil, 'null' => false, 'first' => nil, 'after' => nil, 'primary_key' => nil},
              'key'  => { 'type' => :string,    'limit' => nil, 'precision' => nil, 'scale' => nil, 'default' => nil, 'null' => false, 'first' => nil, 'after' => nil, 'primary_key' => nil},
              'ns'   => { 'type' => :string,  'limit' => nil, 'precision' => nil, 'scale' => nil, 'default' => nil, 'null' => false, 'first' => nil, 'after' => nil, 'primary_key' => true},
              'name' => { 'type' => :string,  'limit' => nil, 'precision' => nil, 'scale' => nil, 'default' => nil, 'null' => false, 'first' => nil, 'after' => nil, 'primary_key' => nil}
          }
      )

    end
  end

end

require 'mongo/document_definition/create'
require 'mongo/document_definition/read'
require 'mongo/document_definition/update'
require 'mongo/document_definition/delete'



