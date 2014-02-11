require 'karras_adapter/version'
require 'mongo/document_definition'

class ActiveRecord::ConnectionAdapters::KarrasAdapter::SchemaCreation < ActiveRecord::ConnectionAdapters::AbstractAdapter::SchemaCreation

  private

  def visit_AlterTable(o)
    sql = Mongo::DocumentDefinition::Update.new(o.name)
    current_fields = Mongo::DocumentDefinition::Read.new(o.name).results.first['fields']
    new_fields = o.adds.inject({}) { |hash, col| hash.merge(visit_AddColumn(col)) }
    sql.document = {'fields' => current_fields.merge(new_fields) }
    sql
  end

  def visit_AddColumn(o)
    # TODO: Verify that this determination of sqltype is or is not needed
    #sql_type = type_to_sql(o.type.to_sym, o.limit, o.precision, o.scale)
    hash = o.to_h
    column = { hash.delete(:name) => hash }
    add_column_options!(column, column_options(o))
  end

  def visit_ColumnDefinition(o)
    raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
  end

  def visit_TableDefinition(o)

    fields = {}
    o.columns.each do |column|
      if column.primary_key
        column.null = false
      elsif column.null.nil?
        column.null = true
      end
      # TODO: Figure out a way to store these as is but change them at the last moment before AR and normal Schema code see's them.
      # Ideally we would want to know something is a hash so we can automatically interpret it as a document or whatever.
      # Any other string marshaled type would work too.
      if column.type == :primary_key
        column.type = :integer
      elsif column.type == :hash
        column.type = :string
      end
      hash = column.to_h
      fields[hash.delete(:name)] = hash
    end
    name = o.name

    Mongo::DocumentDefinition::Create.new(name, fields).tap { |definer| definer.bindings = { 'name' => name, 'fields' => fields } }

  end

  def db
    @conn.db
  end

  def collection(name)
    @conn.collection(name)
  end

  def document_definitions
    @conn.document_definitions
  end

  def schema_migrations
    @conn.schema_migrations
  end

  def system_indexes
    db.system_indexes
  end


end
