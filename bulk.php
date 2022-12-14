<?php
/**
 * UmaTools Bulk Tool
 * 
 * @package   UmaTools
 * @since     1.0.0
 * @author    Ka2
 * @copyright 2022 ka2.org
 * @license   MIT
 */
require_once( __DIR__ . '/vendor/autoload.php' );

use Umamusume\UmaTools\bulkCore;

$class = 'Umamusume\UmaTools\bulkCore';

if ( class_exists( $class ) ) {
    //$instance_1 = new bulkCore();
    //var_dump( $instance_1 );
    $instance = bulkCore::get_object();
    //var_dump( $instance );
    if ( !isset( $argc ) ) {
        die( 'Error: Called from a source other than the command line.' . PHP_EOL );
    }
    if ( $argc > 2 ) {
        $table_name = filter_var( $argv[1], FILTER_SANITIZE_STRING );
        $csv_file   = filter_var( $argv[2], FILTER_SANITIZE_STRING );
        $instance->import_csv( $table_name, $csv_file );
        //$instance->evaluate_talisman_worth();
    } else {
        die( 'Error: Parameters are missing.' . PHP_EOL );
    }
} else {
    trigger_error( "Unable to load class: $class", E_USER_WARNING );
    exit;
}
