require 'mongo/finder'
require 'mongo/inserter'
require 'mongo/updater'
require 'mongo/remover'

module Arel
  module Visitors
    class Karras < Arel::Visitors::ToSql

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
        #[
        #    "DELETE FROM #{visit o.relation}",
        #    ("WHERE #{o.wheres.map { |x| visit x }.join AND}" unless o.wheres.empty?)
        #].compact.join ' '

        remover = Mongo::Remover.new(visit(o.relation))
        remover.query = o.wheres.inject({}) { |kwery, x| kwery.merge(visit(x,a)) }
        remover
      end

      def visit_Arel_Nodes_UpdateStatement(o, a)
#        if o.orders.empty? && o.limit.nil?
#          wheres = o.wheres
#        else
#          key = o.key
#          unless key
#            warn(<<-eowarn) if $VERBOSE
#(#{caller.first}) Using UpdateManager without setting UpdateManager#key is
#deprecated and support will be removed in Arel 4.0.0.  Please set the primary
#key on UpdateManager using UpdateManager#key= '#{key.inspect}'
#            eowarn
#            key = o.relation.primary_key
#          end
#
#          wheres = [Nodes::In.new(key, [build_subselect(key, o)])]
#        end
#
#        [
#            "UPDATE #{visit o.relation, a}",
#            ("SET #{o.values.map { |value| visit value, a }.join ', '}" unless o.values.empty?),
#            ("WHERE #{wheres.map { |x| visit x, a }.join ' AND '}" unless wheres.empty?),
#        ].compact.join ' '

        updater = Mongo::Updater.new(visit(o.relation, a))
        wheres = (o.orders.empty? && o.limit.nil?) ? o.wheres : [Arel::Nodes::In.new(key, [build_subselect(o.key, o)])]
        updater.query = o.wheres.inject({}) { |kwery, x| kwery.merge(visit(x,a)) }
        updater.document = o.values.inject({}) { |sets, value| sets.merge(visit(value,a)) }

        updater
      end

      # TODO: Deal with embedded inserts
      def visit_Arel_Nodes_InsertStatement(o, a)
        #[
        #    "INSERT INTO #{visit o.relation, a}",
        #
        #    ("(#{o.columns.map { |x|
        #      quote_column_name x.name
        #    }.join ', '})" unless o.columns.empty?),
        #
        #    (visit o.values, a if o.values),
        #].compact.join ' '
        #
        fields = o.columns.map { |x| quote_column_name x.name }

        # TODO: Consider active_record/lib/result.rb line 62 for performance
        Mongo::Inserter.new(visit(o.relation, a), Hash[fields.zip(visit(o.values, a))])

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

      def visit_Arel_Nodes_Values(o, a)
        o.expressions.zip(o.columns).map { |value, attr|(Nodes::SqlLiteral === value) ? visit(value, a) : quote(value, attr && column_for(attr)) }
      end

      def visit_Arel_Nodes_SelectStatement(o, a)
        # TODO: Handle with
        if o.with
          visit(o.with, a)
          raise NotImplementedError, 'With not yet implemented'
        end

        # TODO: Handle multiple cores
        selector = visit_Arel_Nodes_SelectCore(o.cores.first, a)

        selector.limit = visit(o.limit, a) if o.limit
        selector.skip = visit(o.offset, a) if o.offset

        if o.lock
          visit(o.lock, a)
          raise NotImplementedError, 'Locking not yet implemented'
        end

        selector
      end

      def visit_Arel_Nodes_SelectCore(o, a)

        # TODO: Handle select from non table sources, ie other inline result sets.
        # TODO: If not the above, remove or deal with !o.source or o.source.empty
        # name = visit(o.source, a) if o.source && !o.source.empty?
        selector = Mongo::Finder.new(visit(o.source, a))

        fields = o.projections.map { |x| visit(x, a) }
        selector.fields = fields unless fields.empty? || fields == ['*']

        selector.query = o.wheres.inject({}) { |kwery, x| kwery.merge(visit(x,a)) }

        selector
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
        visit(o.expr, a)
      end

      def visit_Arel_Nodes_Limit(o, a)
        visit(o.expr, a)
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
          left = visit(o.left, a)
          right = o.right.map { |j| visit j, a }
          unless right.empty?
            [left, right.join(' ')]
          else
            left
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
        super
      end

      def visit_Arel_Nodes_In(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_NotIn(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_And(o, a)
        o.children.map { |x| visit(x, a) }.inject({}) { |total, part| total.merge part }
      end

      def visit_Arel_Nodes_Or(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_Assignment(o, a)
        #right = quote(o.right, column_for(o.left))
        #"#{visit o.left, a} = #{right}"
        {visit(o.left, a) => quote(o.right, column_for(o.left))}
      end

      def visit_Arel_Nodes_Equality(o, a)
        a = o.left if Arel::Attributes::Attribute === o.left
        { visit(o.left, a) => o.right.nil? ? nil : visit(o.right, a) }
      end

      def visit_Arel_Nodes_NotEqual(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_As(o, a)
        raise NotImplementedError, "#{caller_locations(0).first.base_label} not implemented"
      end

      def visit_Arel_Nodes_UnqualifiedColumn(o, a)
        "#{quote_column_name o.name}"
      end

      def visit_Arel_Attributes_Attribute(o, a)
        #join_name = o.relation.table_alias || o.relation.name
        #"#{quote_table_name join_name}.#{quote_column_name o.name}"
        quote_column_name o.name
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
