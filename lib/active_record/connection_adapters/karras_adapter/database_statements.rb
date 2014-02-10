require 'karras_adapter/version'

#TODO: Implement these SQL specific database statements
module ActiveRecord::ConnectionAdapters::KarrasAdapter::DatabaseStatements

  # TODO: Add log statements like other adapters.
  def execute(sql, name = nil)
    if sql.is_a?(Mongo::Operation::Base) || sql.is_a?(Mongo::DocumentDefinition)
      sql.results
    else
      raise NotImplementedError, "#{caller_locations(0).first.base_label} raw commands not implemented"
    end
  end

  def exec_query(sql, name = 'SQL', binds = [])
    if sql.is_a?(Mongo::Operation::Base) || sql.is_a?(Mongo::DocumentDefinition)
      sql.bindings = binds
      sql.results
    else
      raise NotImplementedError, "#{caller_locations(0).first.base_label} raw commands not implemented"
    end
  end



  # Inserts the given fixture into the table. Overridden in adapters that require
  # something beyond a simple insert (eg. Oracle).
  def insert_fixture(fixture, table_name)
    #columns = schema_cache.columns_hash(table_name)
    #
    #key_list   = []
    #value_list = fixture.map do |name, value|
    #  key_list << quote_column_name(name)
    #  quote(value, columns[name])
    #end
    #
    #execute "INSERT INTO #{quote_table_name(table_name)} (#{key_list.join(', ')}) VALUES (#{value_list.join(', ')})", 'Fixture Insert'
    raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
  end

  def empty_insert_statement_value
#        "DEFAULT VALUES"
    raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
  end

  def case_sensitive_equality_operator
    #"="
    raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
  end

  def limited_update_conditions(where_sql, quoted_table_name, quoted_primary_key)
    #"WHERE #{quoted_primary_key} IN (SELECT #{quoted_primary_key} FROM #{quoted_table_name} #{where_sql})"
    raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
  end

  # Returns an array of record hashes with the column names as keys and
  # column values as values.
  def select(sql, name = nil, binds = [])
    exec_query(sql, name, binds)
  end


end
