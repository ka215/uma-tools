<?php
/**
 * Holds final class for bulk user
 *
 * @package UmaTools
 * @since   1.0.0
 */

namespace Umamusume\UmaTools;

if ( !class_exists( 'bulkCore' ) ) :

final class bulkCore extends abstractClass {

    /**
     * Load a trait that defines common methods for database operations
     */
    use DBHelper;

    /**
     * Initialization
     *
     * @access public
     */
    public function init() {
        // Nothing to do
    }

    /**
     * Catch Requests
     *
     * @access protected
     */
    public function catch_request() {
        // Nothing to do
    }


    /**
     * Importing data from a CSV file and registering it in a database
     *
     * @access public
     */
    public function import_csv( string $table_name, string $csv_file ): void {
        if ( !file_exists( $csv_file ) ) {
            die( 'Could not find the CSV file to import.' . PHP_EOL );
        }
        if ( !$this->table_exists( $table_name ) ) {
            die( 'The table specified as the import destination does not exist.' . PHP_EOL );
        }
        $file_path = $this->optimize_file( $csv_file );
        $csv_data = new \SplFileObject( $file_path, 'r' );
        $csv_data->setFlags( \SplFileObject::READ_CSV );
        $base_csv_format = $this->get_csv_format( $table_name );
        $csv_format = array_values( $base_csv_format );
        $columns = array_keys( $base_csv_format );
        $data = [];
        foreach ( $csv_data as $line ) {
            $line = array_filter( $line, function( $item, $index ) use ( &$csv_format ) {
                if ( $index <= count( $csv_format ) ) {
                    return $csv_format[$index]['label'] !== $item && (bool)preg_match( "@{$csv_format[$index]['pattern']}@", $item );
                } else {
                    return false;
                }
            }, ARRAY_FILTER_USE_BOTH );
            if ( empty( $line ) ) {
                continue;
            }
            foreach ( $line as $_idx => $_val ) {
                $_type = $csv_format[$_idx]['type'];
                switch ( true ) {
                    case preg_match( '@^(|tiny|medium|big)int@', $_type ):
                        $line[$_idx] = (int)$_val;
                        break;
                    case $_type === 'json':
                        $line[$_idx] = json_decode( str_replace( "'", '"', $_val ), true );
                        break;
                    case $_type === 'bit(1)':
                        $line[$_idx] = (bool)preg_match( '/^(true|1)$/i', $_val);
                        break;
                    case preg_match( '@^(float|double)@', $_type ):
                        $line[$_idx] = (float)$_val;
                        break;
                    case preg_match( '@^(varchar|text)@', $_type ):
                    default:
                        $line[$_idx] = (string)$_val;
                        break;
                }
            }
            $data[] = $line;
        }
        if ( empty( $data ) ) {
            die( 'Oops, this CSV does not contain any valid data to import.' . PHP_EOL );
        }
        // Discard file that have been read
        if ( $csv_file !== $file_path ) {
            $this->discard_file( $file_path );
        }
        // Truncate table before insertion
        $this->truncate_table( $table_name );
        // Start insertion with transaction
        $counter = 0;
        try {
            $this->dbh->beginTransaction();
            foreach ( $data as $_record ) {
                $target_cols = array_filter( $columns, function( $_k ) use ( &$_record ) {
                    return array_key_exists( $_k, $_record );
                }, ARRAY_FILTER_USE_KEY );
                $one_row_data = array_combine( $target_cols, $_record );
                if ( $this->insert_data( $table_name, $one_row_data ) ) {
                    $counter++;
                }
            }
            $this->dbh->commit();
        } catch ( \PDOException $e ) {
            $this->dbh->rollBack();
            die( 'Error: ' . $e->getMessage() );
        }
        if ( $counter > 0 ) {
            $message = sprintf( '%d/%d data has been completed insertion into the "%s" table.', $counter, count( $data ), $table_name );
        } else {
            $message = 'Failed to insert data.';
        }
        die( $message . PHP_EOL );
    }

    /**
     * Unify character encoding of import files to UTF-8
     *
     * @access private
     */
    private function optimize_file( string $file_path ): string {
        setlocale( LC_ALL, 'ja_JP.UTF-8' );
        $encodings = [ 'ASCII', 'ISO-2022-JP', 'UTF-8', 'eucjp-win', 'Windows-31J', 'sjis-win', 'SJIS' ];
        $_data = @file_get_contents( $file_path );
        $before_encoding = mb_detect_encoding( $_data, $encodings );
        if ( $before_encoding === 'UTF-8' ) {
            return $file_path;
        }
        if ( $before_encoding === false ) {
            $before_encoding = 'SJIS';
        }
        $_data = mb_convert_encoding( $_data, 'UTF-8', $before_encoding );
        $put_file_path = __DIR__ .'/'. md5( microtime( true ) ) .'.csv';
        if ( file_put_contents( $put_file_path, $_data, LOCK_EX ) ) {
            return $put_file_path;
        } else {
            die( 'Oops, this CSV does not contain any valid data to import.' );
        }
    }

    /**
     * Discard specific file
     *
     * @access private
     */
    private function discard_file( string $file_path ): void {
        @unlink( $file_path );
    }

    /**
     * Obtain the data validation schema for the table to be handled
     *
     * @access private
     */
    private function get_csv_format( string $table_name ): array {
        return match ( $table_name ) {
            'races' => [
                'id'                => [ 'type' => 'int(11) unsigned',    'pattern' => '^[0-9]+$',    'label' => '?????????ID' ],
                'grade'             => [ 'type' => 'varchar(6)',          'pattern' => '^.*+$',       'label' => '????????????' ],
                'name'              => [ 'type' => 'varchar(255)',        'pattern' => '^.*+$',       'label' => '????????????' ],
                'junior'            => [ 'type' => 'bit(1)',              'pattern' => '^(TRUE|true|True|FALSE|false|False|0|1)?$', 'label' => '????????????' ],
                'classic'           => [ 'type' => 'bit(1)',              'pattern' => '^(TRUE|true|True|FALSE|false|False|0|1)?$', 'label' => '???????????????' ],
                'senior'            => [ 'type' => 'bit(1)',              'pattern' => '^(TRUE|true|True|FALSE|false|False|0|1)?$', 'label' => '?????????' ],
                'month'             => [ 'type' => 'tinyint(4) unsigned', 'pattern' => '^\d{1,4}$',   'label' => '?????????' ],
                'half_period'       => [ 'type' => 'varchar(1)',          'pattern' => '^.*+$',       'label' => '???????????????' ],
                'period_name'       => [ 'type' => 'varchar(255)',        'pattern' => '^.*+$',       'label' => '????????????' ],
                'place'             => [ 'type' => 'varchar(255)',        'pattern' => '^.*+$',       'label' => '??????' ],
                'ground'            => [ 'type' => 'varchar(255)',        'pattern' => '^.*+$',       'label' => '??????' ],
                'distance'          => [ 'type' => 'int(11) unsigned',    'pattern' => '^[0-9]+$',    'label' => '??????' ],
                'suitable_distance' => [ 'type' => 'varchar(255)',        'pattern' => '^.*+$',       'label' => '????????????' ],
                'direction'         => [ 'type' => 'varchar(1)',          'pattern' => '^.*+$',       'label' => '????????????' ],
                'lane'              => [ 'type' => 'varchar(1)',          'pattern' => '^.*+$',       'label' => '????????????' ],
                'tight'             => [ 'type' => 'bit(1)',              'pattern' => '^(TRUE|true|True|FALSE|false|False|0|1)?$', 'label' => '?????????' ],
                'full_gate'         => [ 'type' => 'tinyint(2) unsigned', 'pattern' => '^\d{1,4}$',   'label' => '???????????????' ],
                'night_race'        => [ 'type' => 'bit(1)',              'pattern' => '^(TRUE|true|True|FALSE|false|False|0|1)?$', 'label' => '????????????' ],
                'season'            => [ 'type' => 'varchar(255)',        'pattern' => '^.*+$',       'label' => '??????' ],
                'meta'              => [ 'type' => 'json',                'pattern' => '^.*?$',       'label' => '???????????????' ],
                'extend'            => [ 'type' => 'text',                'pattern' => '^.*+$',       'label' => '???????????????' ],
            ],
            'courses' => [
                'id'                => [ 'type' => 'int(11) unsigned',    'pattern' => '^[0-9]+$',    'label' => '?????????ID' ],
                'place'             => [ 'type' => 'varchar(255)',        'pattern' => '^.*+$',       'label' => '??????' ],
                'ground'            => [ 'type' => 'varchar(255)',        'pattern' => '^.*+$',       'label' => '??????' ],
                'distance'          => [ 'type' => 'int(11) unsigned',    'pattern' => '^[0-9]+$',    'label' => '??????' ],
                'lane'              => [ 'type' => 'varchar(1)',          'pattern' => '^.*+$',       'label' => '????????????' ],
                'category'          => [ 'type' => 'varchar(255)',        'pattern' => '^.*+$',       'label' => '????????????' ],
                'ref_status'        => [ 'type' => 'json',                'pattern' => '^.*?$',       'label' => '?????????????????????' ],
                'slope'             => [ 'type' => 'json',                'pattern' => '^.*?$',       'label' => '???????????????' ],
                'meta'              => [ 'type' => 'json',                'pattern' => '^.*?$',       'label' => '???????????????' ],
                'extend'            => [ 'type' => 'text',                'pattern' => '^.*+$',       'label' => '???????????????' ],
            ],
            'skills' => [
                'id'                => [ 'type' => 'int(11) unsigned',    'pattern' => '^[0-9]+$',    'label' => '?????????ID' ],
                'name'              => [ 'type' => 'varchar(255)',        'pattern' => '^.*+$',       'label' => '????????????' ],
                'passive'           => [ 'type' => 'bit(1)',              'pattern' => '^(TRUE|true|True|FALSE|false|False|0|1)?$', 'label' => '?????????????????????' ],
                'rare'              => [ 'type' => 'bit(1)',              'pattern' => '^(TRUE|true|True|FALSE|false|False|0|1)?$', 'label' => '???????????????' ],
                'bad'               => [ 'type' => 'bit(1)',              'pattern' => '^(TRUE|true|True|FALSE|false|False|0|1)?$', 'label' => '??????????????????' ],
                'point'             => [ 'type' => 'int(11) unsigned',    'pattern' => '^[0-9]+$',    'label' => '??????????????????' ],
                'speed'             => [ 'type' => 'int(11)',             'pattern' => '^-?[0-9]+$',  'label' => '??????????????????' ],
                'stamina'           => [ 'type' => 'int(11)',             'pattern' => '^-?[0-9]+$',  'label' => '??????????????????' ],
                'power'             => [ 'type' => 'int(11)',             'pattern' => '^-?[0-9]+$',  'label' => '???????????????' ],
                'guts'              => [ 'type' => 'int(11)',             'pattern' => '^-?[0-9]+$',  'label' => '????????????' ],
                'wisdom'            => [ 'type' => 'int(11)',             'pattern' => '^-?[0-9]+$',  'label' => '????????????' ],
                'vision'            => [ 'type' => 'int(11)',             'pattern' => '^-?[0-9]+$',  'label' => '????????????' ],
                'fasten'            => [ 'type' => 'float(5,3)',          'pattern' => '^-?\d{1,2}\.?\d{,3}$', 'label' => '?????????' ],
                'conditions'        => [ 'type' => 'json',                'pattern' => '^.*?$',       'label' => '????????????' ],
                'icon'              => [ 'type' => 'varchar(255)',        'pattern' => '^.*+$',       'label' => '????????????' ],
                'meta'              => [ 'type' => 'json',                'pattern' => '^.*?$',       'label' => '???????????????' ],
                'extend'            => [ 'type' => 'text',                'pattern' => '^.*+$',       'label' => '???????????????' ],
            ],
            'characters' => [
                'id'                => [ 'type' => 'int(11) unsigned',    'pattern' => '^[0-9]+$',    'label' => '??????????????????ID' ],
                'name'              => [ 'type' => 'varchar(255)',        'pattern' => '^.*+$',       'label' => '?????????????????????' ],
                'prefix'            => [ 'type' => 'varchar(255)',        'pattern' => '^.*+$',       'label' => '?????????' ],
                'honorary_name'     => [ 'type' => 'varchar(255)',        'pattern' => '^.*+$',       'label' => '????????????' ],
                'rare'              => [ 'type' => 'tinyint(1) unsigned', 'pattern' => '^\d{1,4}$',   'label' => '?????????' ],
                'turf'              => [ 'type' => 'varchar(1)',          'pattern' => '^[A-G]$',     'label' => '?????????' ],
                'dirt'              => [ 'type' => 'varchar(1)',          'pattern' => '^[A-G]$',     'label' => '???????????????' ],
                'short'             => [ 'type' => 'varchar(1)',          'pattern' => '^[A-G]$',     'label' => '???????????????' ],
                'mile'              => [ 'type' => 'varchar(1)',          'pattern' => '^[A-G]$',     'label' => '???????????????' ],
                'middle'            => [ 'type' => 'varchar(1)',          'pattern' => '^[A-G]$',     'label' => '???????????????' ],
                'long'              => [ 'type' => 'varchar(1)',          'pattern' => '^[A-G]$',     'label' => '???????????????' ],
                'frontrunner'       => [ 'type' => 'varchar(1)',          'pattern' => '^[A-G]$',     'label' => '????????????' ],
                'stalker'           => [ 'type' => 'varchar(1)',          'pattern' => '^[A-G]$',     'label' => '????????????' ],
                'looker'            => [ 'type' => 'varchar(1)',          'pattern' => '^[A-G]$',     'label' => '????????????' ],
                'saverunner'        => [ 'type' => 'varchar(1)',          'pattern' => '^[A-G]$',     'label' => '????????????' ],
                'speed'             => [ 'type' => 'tinyint(4) unsigned', 'pattern' => '^\d{1,4}$',   'label' => '??????????????????' ],
                'stamina'           => [ 'type' => 'tinyint(4) unsigned', 'pattern' => '^\d{1,4}$',   'label' => '??????????????????' ],
                'power'             => [ 'type' => 'tinyint(4) unsigned', 'pattern' => '^\d{1,4}$',   'label' => '???????????????' ],
                'guts'              => [ 'type' => 'tinyint(4) unsigned', 'pattern' => '^\d{1,4}$',   'label' => '????????????' ],
                'wisdom'            => [ 'type' => 'tinyint(4) unsigned', 'pattern' => '^\d{1,4}$',   'label' => '????????????' ],
                'gr_spd'            => [ 'type' => 'tinyint(4) unsigned', 'pattern' => '^\d{1,4}$',   'label' => '?????????????????????' ],
                'gr_stm'            => [ 'type' => 'tinyint(4) unsigned', 'pattern' => '^\d{1,4}$',   'label' => '?????????????????????' ],
                'gr_pow'            => [ 'type' => 'tinyint(4) unsigned', 'pattern' => '^\d{1,4}$',   'label' => '??????????????????' ],
                'gr_gut'            => [ 'type' => 'tinyint(4) unsigned', 'pattern' => '^\d{1,4}$',   'label' => '???????????????' ],
                'gr_wis'            => [ 'type' => 'tinyint(4) unsigned', 'pattern' => '^\d{1,4}$',   'label' => '???????????????' ],
                'native_skill'      => [ 'type' => 'json',                'pattern' => '^.*?$',       'label' => '???????????????' ],
                'skills'            => [ 'type' => 'json',                'pattern' => '^.*?$',       'label' => '?????????' ],
                'target_races'      => [ 'type' => 'json',                'pattern' => '^.*?$',       'label' => '???????????????' ],
                'meta'              => [ 'type' => 'json',                'pattern' => '^.*?$',       'label' => '???????????????' ],
                'extend'            => [ 'type' => 'text',                'pattern' => '^.*+$',       'label' => '???????????????' ],
            ],
            default => [],
        };
    }

}

endif;