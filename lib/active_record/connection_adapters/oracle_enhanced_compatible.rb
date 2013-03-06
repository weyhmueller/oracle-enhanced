# -*- coding: utf-8 -*-
# Compatibility with Rails 3.2
module ActiveRecord
  class Base
    # Establishes a connection to the database that's used by all Active Record objects.
    def self.oracle_enhanced_connection(config) #:nodoc:
      if config[:emulate_oracle_adapter] == true
        # allows the enhanced adapter to look like the OracleAdapter. Useful to pick up
        # conditionals in the rails activerecord test suite
        require 'active_record/connection_adapters/emulation/oracle_adapter'
        ConnectionAdapters::OracleAdapter.new(
          ConnectionAdapters::OracleEnhancedConnection.create(config), logger, config)
      else
        ConnectionAdapters::OracleEnhancedAdapter.new(
          ConnectionAdapters::OracleEnhancedConnection.create(config), logger, config)
      end
    end

    def arel_attributes_values_with_virtual_columns(include_primary_key = true, include_readonly_attributes = true, attribute_names = @attributes.keys)
      virtual_column_names = self.class.virtual_columns.map(&:name)
      arel_attributes_values_without_virtual_columns(include_primary_key, include_readonly_attributes, attribute_names - virtual_column_names)
    end

     alias_method_chain :arel_attributes_values, :virtual_columns
    end
end

module ActiveRecord
  module ConnectionAdapters #:nodoc:
    class OracleEnhancedAdapter < AbstractAdapter
      def columns_without_cache(table_name, name = nil) #:nodoc:
        table_name = table_name.to_s
        # get ignored_columns by original table name
        ignored_columns = ignored_table_columns(table_name)

        (owner, desc_table_name, db_link) = @connection.describe(table_name)

        # reset do_not_prefetch_primary_key cache for this table
        @@do_not_prefetch_primary_key[table_name] = nil

        table_cols = <<-SQL.strip.gsub(/\s+/, ' ')
          SELECT column_name AS name, data_type AS sql_type, data_default, nullable, virtual_column, hidden_column,
                 DECODE(data_type, 'NUMBER', data_precision,
                                   'FLOAT', data_precision,
                                   'VARCHAR2', DECODE(char_used, 'C', char_length, data_length),
                                   'RAW', DECODE(char_used, 'C', char_length, data_length),
                                   'CHAR', DECODE(char_used, 'C', char_length, data_length),
                                    NULL) AS limit,
                 DECODE(data_type, 'NUMBER', data_scale, NULL) AS scale
            FROM all_tab_cols#{db_link}
           WHERE owner      = '#{owner}'
             AND table_name = '#{desc_table_name}'
             AND hidden_column = 'NO'
           ORDER BY column_id
        SQL

        # added deletion of ignored columns
        # add to_a
        select_all(table_cols, name).to_a.delete_if do |row|
          ignored_columns && ignored_columns.include?(row['name'].downcase)
        end.map do |row|
          limit, scale = row['limit'], row['scale']
          if limit || scale
            row['sql_type'] += "(#{(limit || 38).to_i}" + ((scale = scale.to_i) > 0 ? ",#{scale})" : ")")
          end

          is_virtual = row['virtual_column']=='YES'

          # clean up odd default spacing from Oracle
          if row['data_default'] && !is_virtual
            row['data_default'].sub!(/^(.*?)\s*$/, '\1')

            # If a default contains a newline these cleanup regexes need to
            # match newlines.
            row['data_default'].sub!(/^'(.*)'$/m, '\1')
            row['data_default'] = nil if row['data_default'] =~ /^(null|empty_[bc]lob\(\))$/i
          end

          OracleEnhancedColumn.new(oracle_downcase(row['name']),
                           row['data_default'],
                           row['sql_type'],
                           row['nullable'] == 'Y',
                           # pass table name for table specific column definitions
                           table_name,
                           # pass column type if specified in class definition
                           get_type_for_column(table_name, oracle_downcase(row['name'])), is_virtual)
        end
      end

      private

      def select(sql, name = nil, binds = [])
        if ActiveRecord.const_defined?(:Result)
          # add to_a
          exec_query(sql, name, binds).to_a
        else
          log(sql, name) do
            @connection.select(sql, name, false)
          end
        end
      end
    end
  end
end

# remove_column with an array
module ActiveRecord
  module ConnectionAdapters
    module OracleEnhancedSchemaStatements
      def remove_column(table_name, *column_names) #:nodoc:
        raise ArgumentError.new("You must specify at least one column name.  Example: remove_column(:people, :first_name)") if column_names.empty?

        major, minor = ActiveRecord::VERSION::MAJOR, ActiveRecord::VERSION::MINOR
        is_deprecated = (major == 3 and minor >= 2) or major > 3

        if column_names.flatten! and is_deprecated
          message = 'Passing array to remove_columns is deprecated, please use ' +
            'multiple arguments, like: `remove_columns(:posts, :foo, :bar)`'
          ActiveSupport::Deprecation.warn message, caller
        end

        column_names.each do |column_name|
          execute "ALTER TABLE #{quote_table_name(table_name)} DROP COLUMN #{quote_column_name(column_name)}"
        end
        ensure
          clear_table_columns_cache(table_name)
        end
    end
  end
end

# Starting with rails 3.2.9 the method #field_changed?
# was renamed to #_field_changed?
if ActiveRecord::Base.method_defined?(:changed?)
  ActiveRecord::Base.class_eval do
    include ActiveRecord::ConnectionAdapters::OracleEnhancedDirty::InstanceMethods
    if private_method_defined?(:field_changed?)
    alias_method :field_changed?, :_field_changed?
    end
  end
end

module ActiveRecord
  module ConnectionAdapters
    module DatabaseStatements
      def exec_insert(sql, name, binds)
        log(sql, name, binds) do
          returning_id_col = returning_id_index = nil
          cursor = if @statements.key?(sql)
            @statements[sql]
          else
            @statements[sql] = @connection.prepare(sql)
          end

          binds.each_with_index do |bind, i|
            col, val = bind
            if col.returning_id?
              returning_id_col = [col]
              returning_id_index = i + 1
              cursor.bind_returning_param(returning_id_index, Integer)
            else
              cursor.bind_param(i + 1, type_cast(val, col), col && col.type)
            end
          end

          cursor.exec_update

          rows = []
          if returning_id_index
            returning_id = cursor.get_returning_param(returning_id_index, Integer)
            rows << [returning_id]
          end
          ActiveRecord::Result.new(returning_id_col || [], rows)
        end
      end
    end
  end
end
