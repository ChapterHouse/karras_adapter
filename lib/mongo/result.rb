#require 'mongo'
require 'active_record/result'

module Mongo

  class Result < ActiveRecord::Result

    include Enumerable

    attr_reader :results

    def initialize(mongo_cursor)
      @results = mongo_cursor.to_a
    end

    def each
      results.to_a.each { |row| yield row }
    end

    def columns
      # TODO: Should we only return the keys that are present in all results? I dunno. Thats what is making this Mongo adapter so interesting.
      @columns ||= results.map(&:keys).flatten.uniq
    end

    def column_types
      []
    end

    def to_hash
      results
    end

    alias :map! :map
    alias :collect! :map

    def rows
      @rows ||= results.map(&:values)
    end

    def empty?
      results.empty?
    end

    def to_ary
      results
    end

    def [](idx)
      results.last
    end

    def last
      results.last
    end

    def initialize_copy(other)
      @results = other.results.dup
    end

  end
end
