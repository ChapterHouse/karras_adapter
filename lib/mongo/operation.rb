require 'mongo'
require 'mongo/result'

module Mongo

  module Operation

    def self.included(base)
      base.extend(ClassMethods)
    end

    module ClassMethods

      def default_db
        # Convert to a Mongo:DB at the last moment as the connection may not have existed when the db name was given as the default
        @default_db = default_connection.db(@default_db.to_s) if @default_db && !@default_db.is_a?(Mongo::DB)
        self != Mongo::Operation ? (@default_db || Mongo::Operation.default_db) : @default_db
      end

      def default_db=(name_or_db)
        # Convert it now if we can and need to
        name_or_db = default_connection.db(name_or_db.to_s) if name_or_db && default_connection? && !name_or_db.is_a?(Mongo::DB)
        @default_db = name_or_db
        @default_connection = nil
      end

      def default_db?
        !@default_db.nil? || self != Mongo::Operation && Mongo::Operation.default_db?
      end

      def default_connection
        if @default_connection
          @default_connection
        elsif default_db
          default_db.connection
        elsif self != Mongo::Operation
          Mongo::Operation.default_connection
        else
          raise 'No default connection or database has been previously set'
        end
      end

      def default_connection=(connection)
        @default_connection = connection
        @default_db = nil
      end

      def default_connection?
        !@default_connection.nil? || default_db? || self != Mongo::Operation &&  Mongo::Operation.default_connection?
      end

    end

    extend ClassMethods

    def collection(name=nil)
      name ? db.collection(name) : @collection
    end

    def collection=(name_or_collection)
      @collection = name_or_collection.is_a?(Mongo::Connection) ? name_or_collection : collection(name_or_collection)
      @db = nil
      @connection = nil
    end

    def collection?
      !@collection.nil?
    end

    def connection
      if @connection
        @connection
      elsif db?
        db.connection
      else
        default_connection
      end
    end

    def connection=(new_connection)
      @connection = new_connection
      @collection = nil
      @db = nil
    end

    def connection?
      !@connection.nil? || db? || default_connection?
    end

    def db(name=nil)
      if name
        connection.db(nil)
      elsif @db
        @db
      elsif collection
        collection.db
      else
        default_db
      end
    end

    def db=(db_or_name)
      @db = db_or_name.is_a?(Mongo::DB) ? db_or_name : db(db_or_name)
      @collection = nil
      @connection = nil
    end

    def db?
      !@db.nil? || collection? || default_db?
    end

    def default_connection
      self.class.default_connection
    end

    def default_coonnection?
      self.class.default_connection?
    end

    def default_db
      self.class.default_db
    end

    def default_db?
      self.class.default_db?
    end

    class Base

      include Mongo::Operation

      attr_accessor :fields, :bindings

      def initialize(mongo_source)
        case mongo_source
          when Mongo::Collection
            @collection = mongo_source
          when Mongo::DB
            @db = mongo_source
          when Mongo::Connection
            @connection = mongo_source
          else
            raise "Unusable mongo source #{mongo_source.class}"
        end
      end

      def result(bindings=nil)
        self.bindings = bindings if bindings
        Mongo::Result.new(execute, fields)
      end

      alias :results :result


      def execute
        raise NotImplementedError, "Someone forgot to define an execute instance method for class #{self.class.name}"
      end

      def document_definitions
        collection('document_definitions')
      end

    end

    class Crud < Base

      attr_accessor :query, :document, :options

      def self.options(*names)
        names.each do |name|
          name = name.to_s
          option = name.to_sym
          class_eval <<-READER, __FILE__, __LINE__ + 1
            def #{name}
              options[#{option.inspect}]
            end
          READER

          class_eval <<-WRITER, __FILE__, __LINE__ + 1
            def #{name}=(new_value)
              options[#{option.inspect}] = new_value
            end
          WRITER
        end
      end

      def self.bind_to(target=nil)
        if target
          @target = target.to_sym
        elsif @target.nil? && superclass < Mongo::Operation::Base
          superclass.bind_to
        else
          @target
        end
      end

      def initialize(collection_or_name, query={}, document={}, options={})
        collection_or_name = db.collection(collection_or_name.to_s) unless collection_or_name.is_a?(Mongo::Collection)
        @query = query
        @document = document
        @options = options
        super(collection_or_name)
      end

      private

      def bound_data
        case self.class.bind_to
          when :query
            bind query
          when :document
            bind document
          when NilClass
            raise "Binding destination not specified. Please use 'bind_to :query' or 'bind_to :document' in the definition of #{self.class.name}"
          else
            raise "Unknown binding destination #{self.class.bind_to.inspect}. Please use 'bind_to :query' or 'bind_to :document' in the definition of #{self.class.name}"
        end
      end

      def bind(hash)
        hash = hash.dup
        # TODO: Abort if not enough bindings are given
        bindings.each do |binding|
          field = binding.first
          value = binding.last
          field = field.name if field.is_a?(ActiveRecord::ConnectionAdapters::Column)
          hash[field] = value if hash.has_key?(field)
        end if bindings
        hash['_id'] = BSON::ObjectId.from_string(hash.delete('id').to_i.to_s(16)) if hash.has_key?('id')
        hash
      end

    end

  end
end