require 'active_record/connection_adapters/abstract_adapter'
require 'active_record/connection_handling/karras'
require 'arel/visitors/karras'


# You need to map _id to id so the routes will work properly
# Currently no records comming back have an id.


module ActiveRecord
  module ConnectionAdapters
    class KarrasAdapter < AbstractAdapter

      VERSION = "0.0.1"

      def initialize(connection, logger, config)
        super
        #@visitor = unprepared_visitor
        @visitor = Arel::Visitors::Karras.new self
        @db = connection
      end


      attr_reader :db

      #add_index("schema_migrations", :version, {:unique=>true, :name=>"unique_schema_migrations"})
      def add_index(table_name, column_name, options = {})
        collection(table_name).create_index(column_name)
      end

      def columns(table_name)
        document_fields(table_name).map { |name, field_definition|
          Column.new(name, field_definition['default'], field_definition['type'], field_definition['null'])
        }
      end

      # TODO: Add log statements like other adapters.
      def execute(sql, name = nil)
        if sql.is_a?(Proc)
          sql.call
        else
          raise NotImplementedError, "#{caller_locations(0).first.base_label} raw commands not implemented"
        end
      end

      def exec_query(sql, name = 'SQL', binds = [])
        if sql.is_a?(Proc)
          sql.call(binds)
        else
          raise NotImplementedError, "#{caller_locations(0).first.base_label} raw commands not implemented"
        end
      end

      def indexes(table_name, name = nil)
#        class IndexDefinition < Struct.new(:table, :name, :unique, :columns, :lengths, :orders, :where, :type, :using) #:nodoc:


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

          IndexDefinition.new(table_name, index_name, unique, column_names, lengths, orders, where, type, using)
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


      def primary_key(table_name)
        # TODO: Change this to be a pure mongo lookup by digging into document definitions
        # TODO: Manage _id and id
        id_definition = document_fields(table_name).find { |_, field_definition| field_definition['primary_key'] }
        Array(id_definition).first # && id_definition.first || '_id'
      end

      # Returns an array of record hashes with the column names as keys and
      # column values as values.
      def select(sql, name = nil, binds = [])
        execute(sql, name)
      end

      def tables
        db.collection_names
      end

      def schema_creation
        SchemaCreation.new self
      end

      def supports_migrations?
        true
      end

      def method_missing?(name, *args, &block)
        puts "Oh crap #{name.inspect}(#{args.inspect})"
        super
      end

      def collection(name)
        db.collection(name)
      end

      def document_definitions
        unless db.collection_names.include?('document_definitions')
          collection('document_definitions').insert(
              'name' => 'document_definitions',
              'fields' => {
                  'name' =>   { 'type' => :string, 'limit' => nil, 'precision' => nil, 'scale' => nil, 'default' => nil, 'null' => false, 'first' => nil, 'after' => nil, 'primary_key' => true},
                  # TODO: Umm type idunno? Return to this when you get AR to deal with flux types.
                  'fields' => { 'type' => :string,   'limit' => nil, 'precision' => nil, 'scale' => nil, 'default' => nil, 'null' => false, 'first' => nil, 'after' => nil, 'primary_key' => nil}
              }
          )

          document_definitions.insert(
              'name' => 'system.indexes',
              'fields' => {
                  'v'    => { 'type' => :integer, 'limit' => nil, 'precision' => nil, 'scale' => nil, 'default' => nil, 'null' => false, 'first' => nil, 'after' => nil, 'primary_key' => nil},
                  'key'  => { 'type' => :string,    'limit' => nil, 'precision' => nil, 'scale' => nil, 'default' => nil, 'null' => false, 'first' => nil, 'after' => nil, 'primary_key' => nil},
                  'ns'   => { 'type' => :string,  'limit' => nil, 'precision' => nil, 'scale' => nil, 'default' => nil, 'null' => false, 'first' => nil, 'after' => nil, 'primary_key' => true},
                  'name' => { 'type' => :string,  'limit' => nil, 'precision' => nil, 'scale' => nil, 'default' => nil, 'null' => false, 'first' => nil, 'after' => nil, 'primary_key' => nil}
              }
          )

        end

        collection('document_definitions')
      end

      def schema_migrations
        collection('schema_migrations')
      end

      def system_indexes
        collection('system.indexes')
      end


      # TODO: Something quasi dynamic when it's not in the document definitions?
      def document_definition(name)
        document_definitions.find_one( { 'name' => name } )
        #collection('document_definitions').find_one( { name => { '$exists' => true } } )
      end

      def document_fields(name)
        document_definition(name)['fields']
      end

      def document_field_names(name)
        document_definition(name)['fields'].keys
      end

    end
  end
end

require 'active_record/connection_adapters/karras_adapter/schema_creation'


$depth = 0

trace = TracePoint.new(:call, :return, :raise) do |tp|
  if tp.defined_class == ActiveRecord::ConnectionAdapters::KarrasAdapter || tp.defined_class == Arel::Visitors::Karras
    if tp.event == :call
      puts "#{(tp.defined_class.name.split(':').last + '              ')[0..14]}#{'  ' * $depth}#{tp.method_id}"
      $depth += 1
    elsif tp.event == :raise
      puts "#{'**' * $depth}#{tp.raised_exception}"
      $depth -= 1
    else
      $depth -= 1
    end
  end
end

trace.enable

