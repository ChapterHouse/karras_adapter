class ActiveRecord::ConnectionAdapters::KarrasAdapter::SchemaCreation < ActiveRecord::ConnectionAdapters::AbstractAdapter::SchemaCreation

  private

  def visit_AlterTable(o)
    raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
#          Kernel.warn "visit_AlterTable incomplete"
#          super
  end

  def visit_AddColumn(o)
    raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
    #Kernel.warn "visit_AddColumn incomplete"
    #super
  end

  def visit_ColumnDefinition(o)
    raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
    #Kernel.warn "visit_ColumnDefinition incomplete"
    #super
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
      # Any other string marshalled type would work too.
      if column.type == :primary_key
        column.type = :integer
      elsif column.type == :hash
        column.type = :string
      end
      hash = column.to_h
      fields[hash.delete(:name)] = hash
    end
    name = o.name

    -> do
      document_definitions.insert({ 'name' => name, 'fields' => fields })
      db.create_collection(name)
    end
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
