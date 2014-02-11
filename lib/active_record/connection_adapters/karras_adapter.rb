require 'karras_adapter/version'
require 'active_record/connection_handling/karras'
require 'arel/visitors/karras'
require 'mongo/operation'

require 'active_record/connection_adapters/abstract_adapter'

require 'active_record/connection_adapters/karras_adapter/database_limits'
require 'active_record/connection_adapters/karras_adapter/database_statements'
require 'active_record/connection_adapters/karras_adapter/schema_creation'
require 'active_record/connection_adapters/karras_adapter/schema_statements'


module ActiveRecord
  module ConnectionAdapters
    class KarrasAdapter < AbstractAdapter

      include Mongo::Operation
      include KarrasAdapter::DatabaseLimits
      include KarrasAdapter::DatabaseStatements
      include KarrasAdapter::SchemaStatements

      def initialize(connection, logger, config)
        raise ArgumentError, "Unrecognized mongo connector #{connection.class.name}" unless connection.is_a?(Mongo::DB) || connection.is_a?(Mongo::Connection)
        super
        @visitor = Arel::Visitors::Karras.new self
        if !Mongo::Operation.default_connection?
          # Set a default connection/db if neither has been set yet.
          if connection.is_a?(Mongo::DB)
            Mongo::Operation.default_db = connection
          else
            Mongo::Operation.default_connection = connection
          end
        else
          # Else this is a additional connection. Keep track of it locally.
          if connection.is_a?(Mongo::DB)
            self.default_db = connection
          else
            self.default_connection = connection
          end

        end
      end

      # Youa re here, moving these thinsg and just dscovered this one is potentially inside of class TableDefintions in schema_definitions
      def primary_key(table_name)
        # TODO: Change this to be a pure mongo lookup by digging into document definitions
        # TODO: Manage _id and id
        id_definition = Mongo::DocumentDefinition.fields_for(table_name).find { |_, field_definition| field_definition['primary_key'] }
        Array(id_definition).first # && id_definition.first || '_id'
      end

      def tables
        db.collection_names
      end

      def supports_migrations?
        true
      end

      def method_missing?(name, *args, &block)
        puts "Oh crap #{name.inspect}(#{args.inspect})"
        super
      end

      def schema_creation
        SchemaCreation.new self
      end

      ## TODO: Abstract in the same way of DocumentDefinitions. Lets keep all of the direct calls to collection inside of Crud.
      #def schema_migrations
      #  collection('schema_migrations')
      #end
      #
      ## TODO: Abstract in the same way of DocumentDefinitions. Lets keep all of the direct calls to collection inside of Crud.
      #def system_indexes
      #  collection('system.indexes')
      #end

    end
  end
end



$depth = 0

trace = TracePoint.new(:call, :return, :raise) do |tp|
  if tp.defined_class.name.to_s.include?('Karras')
#  if tp.defined_class == ActiveRecord::ConnectionAdapters::KarrasAdapter || tp.defined_class == Arel::Visitors::Karras
    if tp.event == :call

      name = tp.defined_class.name.split('::')
      name.shift until name.first.include?('Karras') || name.first.include?('Arel')
      name = (name.join('::') + ' ' * 100)[0..40]
      puts "#{name}#{'  ' * $depth}#{tp.method_id}"
      $depth += 1
    elsif tp.event == :raise
      puts "#{'**' * $depth}#{tp.raised_exception}"
    else
      $depth -= 1
    end
  end
end

trace.enable

