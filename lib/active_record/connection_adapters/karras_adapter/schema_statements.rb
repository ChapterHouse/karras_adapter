require 'karras_adapter/version'
require 'active_record/connection_adapters/column'

module ActiveRecord::ConnectionAdapters::KarrasAdapter::SchemaStatements

  # Renames a table.
  #
  #   rename_table('octopuses', 'octopi')
  #
  def rename_table(table_name, new_name)
    table_name = quote_table_name(table_name)
    new_name = quote_table_name(new_name)
    # TODO: Centralize all quote_table_name_calls
    collection(table_name).rename(new_name)
  end

  # Drops a table from the database.
  #
  # Although this command ignores +options+ and the block if one is given, it can be helpful
  # to provide these in a migration's +change+ method so it can be reverted.
  # In that case, +options+ and the block will be used by create_table.
  def drop_table(table_name, options = {})
    collection(table_name).drop
  end

  # Removes the column from the table definition.
  #
  #   remove_column(:suppliers, :qualification)
  #
  # The +type+ and +options+ parameters will be ignored if present. It can be helpful
  # to provide these in a migration's +change+ method so it can be reverted.
  # In that case, +type+ and +options+ will be used by add_column.
  def remove_column(table_name, column_name, type = nil, options = {})
    # TODO: Should this just be a document definition change or should all documents in the collection be udpated?
    #execute "ALTER TABLE #{quote_table_name(table_name)} DROP #{quote_column_name(column_name)}"
    raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
  end

  # Changes the column's definition according to the new options.
  # See TableDefinition#column for details of the options you can use.
  #
  #   change_column(:suppliers, :name, :string, limit: 80)
  #   change_column(:accounts, :description, :text)
  #
  def change_column(table_name, column_name, type, options = {})
    raise NotImplementedError, "change_column is not implemented"
  end

  # Sets a new default value for a column:
  #
  #   change_column_default(:suppliers, :qualification, 'new')
  #   change_column_default(:accounts, :authorized, 1)
  #
  # Setting the default to +nil+ effectively drops the default:
  #
  #   change_column_default(:users, :email, nil)
  #
  def change_column_default(table_name, column_name, default)
    raise NotImplementedError, "change_column_default is not implemented"
  end

  # Sets or removes a +NOT NULL+ constraint on a column. The +null+ flag
  # indicates whether the value can be +NULL+. For example
  #
  #   change_column_null(:users, :nickname, false)
  #
  # says nicknames cannot be +NULL+ (adds the constraint), whereas
  #
  #   change_column_null(:users, :nickname, true)
  #
  # allows them to be +NULL+ (drops the constraint).
  #
  # The method accepts an optional fourth argument to replace existing
  # +NULL+s with some other value. Use that one when enabling the
  # constraint if needed, since otherwise those rows would not be valid.
  #
  # Please note the fourth argument does not set a column's default.
  def change_column_null(table_name, column_name, null, default = nil)
    raise NotImplementedError, "change_column_null is not implemented"
  end

  # Renames a column.
  #
  #   rename_column(:suppliers, :description, :name)
  #
  def rename_column(table_name, column_name, new_column_name)
    raise NotImplementedError, "rename_column is not implemented"
  end

  def columns(table_name)
    Mongo::DocumentDefinition.fields_for(table_name).map { |name, field_definition|
      ActiveRecord::ConnectionAdapters::Column.new(name, field_definition['default'], field_definition['type'], field_definition['null'])
    }
  end

  # TODO: Abstract in the same way of DocumentDefinitions. Lets keep all of the direct calls to collection inside of Crud.
  def indexes(table_name, name = nil)
#        Class IndexDefinition < Struct.new(:table, :name, :unique, :columns, :lengths, :orders, :where, :type, :using) #:nodoc:


    collection(table_name).index_information.map { |info|
      info = info.last
      index_name = info['name']
      unique = info['unique']
      column_names = info['key'].keys
      lengths = []
      orders = {} # This might be able to use the 1, -1 key values from info['key']
      where = nil
      type = nil
      using = nil

      ActiveRecord::ConnectionAdapters::IndexDefinition.new(table_name, index_name, unique, column_names, lengths, orders, where, type, using)
    }
#
#system_indexes.find( { :ns => 'test.schema_migrations' }).map { |row|
#IndexDefinition.new(
#    table_name,
#    row['name'],
#    row['unique'] != 0,
#    exec_query("PRAGMA index_info('#{row['name']}')", "SCHEMA").map { |col|
#      col['name']
#    })
#}

  end

  #http://mongodb.github.io/node-mongodb-native/api-articles/nodekoarticle1.html
  def native_database_types #:nodoc:
=begin
    Float is a 8 byte and is directly convertible to the Javascript type Number
    Double class a special class representing a float value, this is especially useful when using capped collections where you need to ensure your values are always floats.
    Integers is a bit trickier due to the fact that Javascript represents all Numbers as 64 bit floats meaning that the maximum integer value is at a 53 bit. Mongo has two types for integers, a 32 bit and a 64 bit. The driver will try to fit the value into 32 bits if it can and promote it to 64 bits if it has to. Similarly it will deserialize attempting to fit it into 53 bits if it can. If it cannot it will return an instance of Long to avoid loosing precession.
    Long class a special class that let’s you store 64 bit integers and also let’s you operate on the 64 bits integers.
    Date maps directly to a Javascript Date
    RegExp maps directly to a Javascript RegExp
    String maps directly to a Javascript String (encoded in utf8)
    Binary class a special class that let’s you store data in Mongo DB
    Code class a special class that let’s you store javascript functions in Mongo DB, can also provide a scope to run the method in
    ObjectID class a special class that holds a MongoDB document identifier (the equivalent to a Primary key)
    DbRef class a special class that let’s you include a reference in a document pointing to another object
    Symbol class a special class that let’s you specify a symbol, not really relevant for javascript but for languages that supports the concept of symbols.
=end
    {
        #:primary_key => default_primary_key_type,
        :primary_key => { :name => 'ObjectID' },
        :string      => { :name => 'String' },
        :text        => { :name => 'String' },
        :integer     => { :name => 'Long'}, # For now just skip the integer promition issues
        :float       => { :name => 'Float' },
        :decimal     => { :name => 'Float' }, # Hrm, do decimal? Is float really the right decision?
        :datetime    => { :name => 'Date' },
        :timestamp   => { :name => 'Date' },
        :time        => { :name => 'Date' },
        :date        => { :name => 'Date' },
        :binary      => { :name => 'Binary' },
        :boolean     => { :name => 'Boolean' },
        :symbol      => { :name => 'Symbol' },
        :regexp      => { :name => 'RegExp' },
        :double      => { :name => 'Double' },
        :dbref       => { :name => 'DbRef' },
        :hash        => { :name => 'String' },
        :flux        => { }
    }
  end



  # Adds a new index to the table. +column_name+ can be a single Symbol, or
  # an Array of Symbols.
  #
  # The index will be named after the table and the column name(s), unless
  # you pass <tt>:name</tt> as an option.
  #
  # ====== Creating a simple index
  #
  #   add_index(:suppliers, :name)
  #
  # generates:
  #
  #   CREATE INDEX suppliers_name_index ON suppliers(name)
  #
  # ====== Creating a unique index
  #
  #   add_index(:accounts, [:branch_id, :party_id], unique: true)
  #
  # generates:
  #
  #   CREATE UNIQUE INDEX accounts_branch_id_party_id_index ON accounts(branch_id, party_id)
  #
  # ====== Creating a named index
  #
  #   add_index(:accounts, [:branch_id, :party_id], unique: true, name: 'by_branch_party')
  #
  # generates:
  #
  #  CREATE UNIQUE INDEX by_branch_party ON accounts(branch_id, party_id)
  #
  # ====== Creating an index with specific key length
  #
  #   add_index(:accounts, :name, name: 'by_name', length: 10)
  #
  # generates:
  #
  #   CREATE INDEX by_name ON accounts(name(10))
  #
  #   add_index(:accounts, [:name, :surname], name: 'by_name_surname', length: {name: 10, surname: 15})
  #
  # generates:
  #
  #   CREATE INDEX by_name_surname ON accounts(name(10), surname(15))
  #
  # Note: SQLite doesn't support index length.
  #
  # ====== Creating an index with a sort order (desc or asc, asc is the default)
  #
  #   add_index(:accounts, [:branch_id, :party_id, :surname], order: {branch_id: :desc, party_id: :asc})
  #
  # generates:
  #
  #   CREATE INDEX by_branch_desc_party ON accounts(branch_id DESC, party_id ASC, surname)
  #
  # Note: MySQL doesn't yet support index order (it accepts the syntax but ignores it).
  #
  # ====== Creating a partial index
  #
  #   add_index(:accounts, [:branch_id, :party_id], unique: true, where: "active")
  #
  # generates:
  #
  #   CREATE UNIQUE INDEX index_accounts_on_branch_id_and_party_id ON accounts(branch_id, party_id) WHERE active
  #
  # ====== Creating an index with a specific method
  #
  #   add_index(:developers, :name, using: 'btree')
  #
  # generates:
  #
  #   CREATE INDEX index_developers_on_name ON developers USING btree (name) -- PostgreSQL
  #   CREATE INDEX index_developers_on_name USING btree ON developers (name) -- MySQL
  #
  # Note: only supported by PostgreSQL and MySQL
  #
  # ====== Creating an index with a specific type
  #
  #   add_index(:developers, :name, type: :fulltext)
  #
  # generates:
  #
  #   CREATE FULLTEXT INDEX index_developers_on_name ON developers (name) -- MySQL
  #
  # Note: only supported by MySQL. Supported: <tt>:fulltext</tt> and <tt>:spatial</tt> on MyISAM tables.
  def add_index(table_name, column_name, options = {})
    #index_name, index_type, index_columns, index_options = add_index_options(table_name, column_name, options)
    #execute "CREATE #{index_type} INDEX #{quote_column_name(index_name)} ON #{quote_table_name(table_name)} (#{index_columns})#{index_options}"
    # TODO: Abstract in the same way of DocumentDefinitions. Lets keep all of the direct calls to collection inside of Crud.
    collection(table_name).create_index(column_name)
  end

  def remove_index!(table_name, index_name) #:nodoc:
    #execute "DROP INDEX #{quote_column_name(index_name)} ON #{quote_table_name(table_name)}"
    collection(table_name).drop_index(column_name)
  end

  def dump_schema_information #:nodoc:
    #sm_table = ActiveRecord::Migrator.schema_migrations_table_name
    #
    #ActiveRecord::SchemaMigration.order('version').map { |sm|
    #  "INSERT INTO #{sm_table} (version) VALUES ('#{sm.version}');"
    #}.join "\n\n"
    raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
  end

  def assume_migrated_upto_version(version, migrations_paths = ActiveRecord::Migrator.migrations_paths)
    #migrations_paths = Array(migrations_paths)
    #version = version.to_i
    #sm_table = quote_table_name(ActiveRecord::Migrator.schema_migrations_table_name)
    #
    #migrated = select_values("SELECT version FROM #{sm_table}").map { |v| v.to_i }
    #paths = migrations_paths.map {|p| "#{p}/[0-9]*_*.rb" }
    #versions = Dir[*paths].map do |filename|
    #  filename.split('/').last.split('_').first.to_i
    #end
    #
    #unless migrated.include?(version)
    #  execute "INSERT INTO #{sm_table} (version) VALUES ('#{version}')"
    #end
    #
    #inserted = Set.new
    #(versions - migrated).each do |v|
    #  if inserted.include?(v)
    #    raise "Duplicate migration #{v}. Please renumber your migrations to resolve the conflict."
    #  elsif v < version
    #    execute "INSERT INTO #{sm_table} (version) VALUES ('#{v}')"
    #    inserted << v
    #  end
    #end
    raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
  end

  def add_column_options!(sql, options) #:nodoc:
    #sql << " DEFAULT #{quote(options[:default], options[:column])}" if options_include_default?(options)
    ## must explicitly check for :null to allow change_column to work on migrations
    #if options[:null] == false
    #  sql << " NOT NULL"
    #end
    #if options[:auto_increment] == true
    #  sql << " AUTO_INCREMENT"
    #end

    fields = {}
    column = options[:column]
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
    fields
  end

  # TODO: Remove when removed from active_record/connection_adapters/abstract/schema_statements.rb
  def distinct(columns, order_by)
    raise NotImplementedError, "#{caller_locations(0).first.base_label} will never be implemented due to deprecation."
  end

  protected
  def add_index_options(table_name, column_name, options = {})
    #column_names = Array(column_name)
    #index_name   = index_name(table_name, column: column_names)
    #
    #if Hash === options # legacy support, since this param was a string
    #  options.assert_valid_keys(:unique, :order, :name, :where, :length, :internal, :using, :algorithm, :type)
    #
    #  index_type = options[:unique] ? "UNIQUE" : ""
    #  index_type = options[:type].to_s if options.key?(:type)
    #  index_name = options[:name].to_s if options.key?(:name)
    #  max_index_length = options.fetch(:internal, false) ? index_name_length : allowed_index_name_length
    #
    #  if options.key?(:algorithm)
    #    algorithm = index_algorithms.fetch(options[:algorithm]) {
    #      raise ArgumentError.new("Algorithm must be one of the following: #{index_algorithms.keys.map(&:inspect).join(', ')}")
    #    }
    #  end
    #
    #  using = "USING #{options[:using]}" if options[:using].present?
    #
    #  if supports_partial_index?
    #    index_options = options[:where] ? " WHERE #{options[:where]}" : ""
    #  end
    #else
    #  if options
    #    message = "Passing a string as third argument of `add_index` is deprecated and will" +
    #        " be removed in Rails 4.1." +
    #        " Use add_index(#{table_name.inspect}, #{column_name.inspect}, unique: true) instead"
    #
    #    ActiveSupport::Deprecation.warn message
    #  end
    #
    #  index_type = options
    #  max_index_length = allowed_index_name_length
    #  algorithm = using = nil
    #end
    #
    #if index_name.length > max_index_length
    #  raise ArgumentError, "Index name '#{index_name}' on table '#{table_name}' is too long; the limit is #{max_index_length} characters"
    #end
    #if index_name_exists?(table_name, index_name, false)
    #  raise ArgumentError, "Index name '#{index_name}' on table '#{table_name}' already exists"
    #end
    #index_columns = quoted_columns_for_index(column_names, options).join(", ")
    #
    #[index_name, index_type, index_columns, index_options, algorithm, using]
    raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
  end



  # TODO: Remove when removed from active_record/connection_adapters/abstract/schema_statements.rb
  def columns_for_remove(table_name, *column_names)
    raise NotImplementedError, "#{caller_locations(0).first.base_label} will never be implemented due to deprecation."
  end

end
