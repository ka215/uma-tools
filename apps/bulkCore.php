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
                'id'                => [ 'type' => 'int(11) unsigned',    'pattern' => '^[0-9]+$',    'label' => 'レースID' ],
                'grade'             => [ 'type' => 'varchar(6)',          'pattern' => '^.*+$',       'label' => 'グレード' ],
                'name'              => [ 'type' => 'varchar(255)',        'pattern' => '^.*+$',       'label' => 'レース名' ],
                'junior'            => [ 'type' => 'bit(1)',              'pattern' => '^(TRUE|true|True|FALSE|false|False|0|1)?$', 'label' => 'ジュニア' ],
                'classic'           => [ 'type' => 'bit(1)',              'pattern' => '^(TRUE|true|True|FALSE|false|False|0|1)?$', 'label' => 'クラシック' ],
                'senior'            => [ 'type' => 'bit(1)',              'pattern' => '^(TRUE|true|True|FALSE|false|False|0|1)?$', 'label' => 'シニア' ],
                'month'             => [ 'type' => 'tinyint(4) unsigned', 'pattern' => '^\d{1,4}$',   'label' => '開催月' ],
                'half_period'       => [ 'type' => 'varchar(1)',          'pattern' => '^.*+$',       'label' => '前半／後半' ],
                'period_name'       => [ 'type' => 'varchar(255)',        'pattern' => '^.*+$',       'label' => '開催時期' ],
                'place'             => [ 'type' => 'varchar(255)',        'pattern' => '^.*+$',       'label' => '場所' ],
                'ground'            => [ 'type' => 'varchar(255)',        'pattern' => '^.*+$',       'label' => 'バ場' ],
                'distance'          => [ 'type' => 'int(11) unsigned',    'pattern' => '^[0-9]+$',    'label' => '距離' ],
                'suitable_distance' => [ 'type' => 'varchar(255)',        'pattern' => '^.*+$',       'label' => '適性距離' ],
                'direction'         => [ 'type' => 'varchar(1)',          'pattern' => '^.*+$',       'label' => '周回方向' ],
                'lane'              => [ 'type' => 'varchar(1)',          'pattern' => '^.*+$',       'label' => '周回形式' ],
                'tight'             => [ 'type' => 'bit(1)',              'pattern' => '^(TRUE|true|True|FALSE|false|False|0|1)?$', 'label' => '小回り' ],
                'full_gate'         => [ 'type' => 'tinyint(2) unsigned', 'pattern' => '^\d{1,4}$',   'label' => 'フルゲート' ],
                'night_race'        => [ 'type' => 'bit(1)',              'pattern' => '^(TRUE|true|True|FALSE|false|False|0|1)?$', 'label' => 'ナイター' ],
                'season'            => [ 'type' => 'varchar(255)',        'pattern' => '^.*+$',       'label' => '季節' ],
                'meta'              => [ 'type' => 'json',                'pattern' => '^.*?$',       'label' => 'メタデータ' ],
                'extend'            => [ 'type' => 'text',                'pattern' => '^.*+$',       'label' => '拡張データ' ],
            ],
            'courses' => [
                'id'                => [ 'type' => 'int(11) unsigned',    'pattern' => '^[0-9]+$',    'label' => 'コースID' ],
                'place'             => [ 'type' => 'varchar(255)',        'pattern' => '^.*+$',       'label' => '場所' ],
                'ground'            => [ 'type' => 'varchar(255)',        'pattern' => '^.*+$',       'label' => 'バ場' ],
                'distance'          => [ 'type' => 'int(11) unsigned',    'pattern' => '^[0-9]+$',    'label' => '距離' ],
                'lane'              => [ 'type' => 'varchar(1)',          'pattern' => '^.*+$',       'label' => '周回形式' ],
                'category'          => [ 'type' => 'varchar(255)',        'pattern' => '^.*+$',       'label' => '適性距離' ],
                'ref_status'        => [ 'type' => 'json',                'pattern' => '^.*?$',       'label' => '参照ステータス' ],
                'slope'             => [ 'type' => 'json',                'pattern' => '^.*?$',       'label' => '坂道データ' ],
                'meta'              => [ 'type' => 'json',                'pattern' => '^.*?$',       'label' => 'メタデータ' ],
                'extend'            => [ 'type' => 'text',                'pattern' => '^.*+$',       'label' => '拡張データ' ],
            ],
            'skills' => [
                'id'                => [ 'type' => 'int(11) unsigned',    'pattern' => '^[0-9]+$',    'label' => 'スキルID' ],
                'name'              => [ 'type' => 'varchar(255)',        'pattern' => '^.*+$',       'label' => 'スキル名' ],
                'passive'           => [ 'type' => 'bit(1)',              'pattern' => '^(TRUE|true|True|FALSE|false|False|0|1)?$', 'label' => 'パッシブスキル' ],
                'rare'              => [ 'type' => 'bit(1)',              'pattern' => '^(TRUE|true|True|FALSE|false|False|0|1)?$', 'label' => 'レアスキル' ],
                'bad'               => [ 'type' => 'bit(1)',              'pattern' => '^(TRUE|true|True|FALSE|false|False|0|1)?$', 'label' => 'バッドスキル' ],
                'point'             => [ 'type' => 'int(11) unsigned',    'pattern' => '^[0-9]+$',    'label' => '必要ポイント' ],
                'speed'             => [ 'type' => 'int(11)',             'pattern' => '^-?[0-9]+$',  'label' => 'スピード補正' ],
                'stamina'           => [ 'type' => 'int(11)',             'pattern' => '^-?[0-9]+$',  'label' => 'スタミナ補正' ],
                'power'             => [ 'type' => 'int(11)',             'pattern' => '^-?[0-9]+$',  'label' => 'パワー補正' ],
                'guts'              => [ 'type' => 'int(11)',             'pattern' => '^-?[0-9]+$',  'label' => '根性補正' ],
                'wisdom'            => [ 'type' => 'int(11)',             'pattern' => '^-?[0-9]+$',  'label' => '賢さ補正' ],
                'vision'            => [ 'type' => 'int(11)',             'pattern' => '^-?[0-9]+$',  'label' => '賢さ補正' ],
                'fasten'            => [ 'type' => 'float(5,3)',          'pattern' => '^-?\d{1,2}\.?\d{,3}$', 'label' => '加速度' ],
                'conditions'        => [ 'type' => 'json',                'pattern' => '^.*?$',       'label' => '発動条件' ],
                'icon'              => [ 'type' => 'varchar(255)',        'pattern' => '^.*+$',       'label' => 'アイコン' ],
                'meta'              => [ 'type' => 'json',                'pattern' => '^.*?$',       'label' => 'メタデータ' ],
                'extend'            => [ 'type' => 'text',                'pattern' => '^.*+$',       'label' => '拡張データ' ],
            ],
            'characters' => [
                'id'                => [ 'type' => 'int(11) unsigned',    'pattern' => '^[0-9]+$',    'label' => 'キャラクターID' ],
                'name'              => [ 'type' => 'varchar(255)',        'pattern' => '^.*+$',       'label' => 'キャラクター名' ],
                'prefix'            => [ 'type' => 'varchar(255)',        'pattern' => '^.*+$',       'label' => '二つ名' ],
                'honorary_name'     => [ 'type' => 'varchar(255)',        'pattern' => '^.*+$',       'label' => '固有称号' ],
                'rare'              => [ 'type' => 'tinyint(1) unsigned', 'pattern' => '^\d{1,4}$',   'label' => 'レア度' ],
                'turf'              => [ 'type' => 'varchar(1)',          'pattern' => '^[A-G]$',     'label' => '芝適性' ],
                'dirt'              => [ 'type' => 'varchar(1)',          'pattern' => '^[A-G]$',     'label' => 'ダート適性' ],
                'short'             => [ 'type' => 'varchar(1)',          'pattern' => '^[A-G]$',     'label' => '短距離適性' ],
                'mile'              => [ 'type' => 'varchar(1)',          'pattern' => '^[A-G]$',     'label' => 'マイル適性' ],
                'middle'            => [ 'type' => 'varchar(1)',          'pattern' => '^[A-G]$',     'label' => '中距離適性' ],
                'long'              => [ 'type' => 'varchar(1)',          'pattern' => '^[A-G]$',     'label' => '長距離適性' ],
                'frontrunner'       => [ 'type' => 'varchar(1)',          'pattern' => '^[A-G]$',     'label' => '逃げ脚質' ],
                'stalker'           => [ 'type' => 'varchar(1)',          'pattern' => '^[A-G]$',     'label' => '先行脚質' ],
                'looker'            => [ 'type' => 'varchar(1)',          'pattern' => '^[A-G]$',     'label' => '差し脚質' ],
                'saverunner'        => [ 'type' => 'varchar(1)',          'pattern' => '^[A-G]$',     'label' => '追込脚質' ],
                'speed'             => [ 'type' => 'tinyint(4) unsigned', 'pattern' => '^\d{1,4}$',   'label' => '初期スピード' ],
                'stamina'           => [ 'type' => 'tinyint(4) unsigned', 'pattern' => '^\d{1,4}$',   'label' => '初期スタミナ' ],
                'power'             => [ 'type' => 'tinyint(4) unsigned', 'pattern' => '^\d{1,4}$',   'label' => '初期パワー' ],
                'guts'              => [ 'type' => 'tinyint(4) unsigned', 'pattern' => '^\d{1,4}$',   'label' => '初期根性' ],
                'wisdom'            => [ 'type' => 'tinyint(4) unsigned', 'pattern' => '^\d{1,4}$',   'label' => '初期賢さ' ],
                'gr_spd'            => [ 'type' => 'tinyint(4) unsigned', 'pattern' => '^\d{1,4}$',   'label' => 'スピード成長率' ],
                'gr_stm'            => [ 'type' => 'tinyint(4) unsigned', 'pattern' => '^\d{1,4}$',   'label' => 'スタミナ成長率' ],
                'gr_pow'            => [ 'type' => 'tinyint(4) unsigned', 'pattern' => '^\d{1,4}$',   'label' => 'パワー成長率' ],
                'gr_gut'            => [ 'type' => 'tinyint(4) unsigned', 'pattern' => '^\d{1,4}$',   'label' => '根性成長率' ],
                'gr_wis'            => [ 'type' => 'tinyint(4) unsigned', 'pattern' => '^\d{1,4}$',   'label' => '賢さ成長率' ],
                'native_skill'      => [ 'type' => 'json',                'pattern' => '^.*?$',       'label' => '固有スキル' ],
                'skills'            => [ 'type' => 'json',                'pattern' => '^.*?$',       'label' => 'スキル' ],
                'target_races'      => [ 'type' => 'json',                'pattern' => '^.*?$',       'label' => '目標レース' ],
                'meta'              => [ 'type' => 'json',                'pattern' => '^.*?$',       'label' => 'メタデータ' ],
                'extend'            => [ 'type' => 'text',                'pattern' => '^.*+$',       'label' => '拡張データ' ],
            ],
            default => [],
        };
    }

}

endif;