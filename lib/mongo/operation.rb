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

      def result(bindings=[])
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

  end
end