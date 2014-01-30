#
#class Object
#
#  def track(&block)
#    location = caller_locations(1).first.base_label
#    puts "#{$depth * '  '}#{location}"
#    $depth += 1
#    begin
#      yield
#    ensure
#      $depth -= 1
#    end
#  end
#
#end

module Arel
  module Visitors
    class Karras < Arel::Visitors::ToSql

      def visit_Arel_Nodes_SelectStatement(o, a)

        visit_Arel_Nodes_SelectCore(o.cores.first, a)

        #str = ''
        #
        #if o.with
        #  str << visit(o.with, a)
        #  str << SPACE
        #end
        #
        #o.cores.each { |x| str << visit_Arel_Nodes_SelectCore(x, a) }
        #
        #unless o.orders.empty?
        #  str << SPACE
        #  str << ORDER_BY
        #  len = o.orders.length - 1
        #  o.orders.each_with_index { |x, i|
        #    str << visit(x, a)
        #    str << COMMA unless len == i
        #  }
        #end
        #
        #str << " #{visit(o.limit, a)}" if o.limit
        #str << " #{visit(o.offset, a)}" if o.offset
        #str << " #{visit(o.lock, a)}" if o.lock
        #
        #str.strip!
        #str
      end

      def visit_Arel_Nodes_SelectCore(o, a)

        # TODO: Handle select from non table sources, ie other inline result sets.
        name = visit(o.source, a) if o.source && !o.source.empty?

        -> do
          b = collection(name).find.to_a

          if b.size > 0
            dsfjkldsjfkldjs
          else
            ActiveRecord::Result.new([], [])
          end


          ## TODO: deal with multiple bind arrays
          #bindings.first.each_slice(2) do |column, value|
          #  # TODO: Is the conditional necessary or will we always be fed sane stuff?
          #  document[column.name] = value if document[column.name] == '?'
          #end
          #collection(collection_name).insert(document)
          #
          #
          #ActiveRecord::Result.new(document.keys.map(&:to_s), [document.values])

        end
=begin
        str = "SELECT"

        str << " #{visit(o.top, a)}"            if o.top
        str << " #{visit(o.set_quantifier, a)}" if o.set_quantifier

        unless o.projections.empty?
          str << SPACE
          len = o.projections.length - 1
          o.projections.each_with_index do |x, i|
            str << visit(x, a)
            str << COMMA unless len == i
          end
        end

        str << " FROM #{visit(o.source, a)}" if o.source && !o.source.empty?

        unless o.wheres.empty?
          str << WHERE
          len = o.wheres.length - 1
          o.wheres.each_with_index do |x, i|
            str << visit(x, a)
            str << AND unless len == i
          end
        end

        unless o.groups.empty?
          str << GROUP_BY
          len = o.groups.length - 1
          o.groups.each_with_index do |x, i|
            str << visit(x, a)
            str << COMMA unless len == i
          end
        end

        str << " #{visit(o.having, a)}" if o.having

        unless o.windows.empty?
          str << WINDOW
          len = o.windows.length - 1
          o.windows.each_with_index do |x, i|
            str << visit(x, a)
            str << COMMA unless len == i
          end
        end

        str
=end
      end




      private

      def connection
        @connection
      end

      def collection(name)
        connection.collection(name)
      end

      def document_definition(name)
        connection.document_definition(name)
      end

      def document_fields(name)
        connection.document_fields(name)
      end

      def document_field_names(name)
        connection.document_field_names(name)
      end

      def visit_Arel_Nodes_DeleteStatement(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_UpdateStatement(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      # TODO: Deal with embedded inserts
      def visit_Arel_Nodes_InsertStatement(o, a)
        collection_name = visit(o.relation, a)
        keys = o.columns.map { |x| quote_column_name x.name }
        values = visit(o.values, a)
        # TODO: Consider active_record/lib/result.rb line 62 for performance
        document = Hash[keys.zip(values)]
        ->(bindings) do
          # TODO: deal with multiple bind arrays
          bindings.first.each_slice(2) do |column, value|
            # TODO: Is the conditional necessary or will we always be fed sane stuff?
            document[column.name] = value if document[column.name] == '?'
          end
          collection(collection_name).insert(document)
          ActiveRecord::Result.new(document.keys.map(&:to_s), [document.values])
        end
      end

      def visit_Arel_Nodes_Exists(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_True(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_False(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      # TODO: If this is only called from one or a few locations then we might do this inline instead.
      def visit_Arel_Nodes_Values(o, a)
        o.expressions.zip(o.columns).map { |value, attr|
          if Nodes::SqlLiteral === value
            visit value, a
          else
            quote(value, attr && column_for(attr))
          end
        }
        #raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_Bin(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_Distinct(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_DistinctOn(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_With(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_WithRecursive(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_Union(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_UnionAll(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_Intersect(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_Except(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_NamedWindow(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_Window(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_Rows(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_Range(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_Preceding(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_Following(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_CurrentRow(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_Over(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_Having(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_Offset(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_Limit(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      # FIXME: this does nothing on most databases, but does on MSSQL
      def visit_Arel_Nodes_Top(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_Lock(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_Grouping(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_SelectManager(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_Ascending(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_Descending(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_Group(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_NamedFunction(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_Extract(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_Count(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_Sum(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_Max(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_Min(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_Avg(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_TableAlias(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_Between(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_GreaterThanOrEqual(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_GreaterThan(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_LessThanOrEqual(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_LessThan(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_Matches(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_DoesNotMatch(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_JoinSource(o, a)
        # Temp work around as I figure out what is going on.
        if o.left
          if o.right
            [
                visit(o.left, a),
                o.right.map { |j| visit j, a }.join(' ')
            ]
          else
            visit(o.left, a)
          end
        else
          o.right.map { |j| visit j, a }.join(' ')
        end
        # This is the original. Still working out what is going on here.
        #  [
        #      (visit(o.left, a) if o.left),
        #      o.right.map { |j| visit j, a }.join(' ')
        #  ].compact
      end

      def visit_Arel_Nodes_StringJoin(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_OuterJoin(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_InnerJoin(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_On(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_Not(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Table(o, a)
        rc = super
        rc
      end

      def visit_Arel_Nodes_In(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_NotIn(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_And(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_Or(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_Assignment(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_Equality(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_NotEqual(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_As(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_UnqualifiedColumn(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Attributes_Attribute(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_InfixOperation(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Array(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

    end
  end
end
