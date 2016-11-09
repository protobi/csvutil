var CSV = require('csv');
var fs = require('fs');
var spssConvert = require('/Users/pieter/Exp/spss-convert');
var async = require('async')

module.exports = {
	read_csv: function(filepath, callback) {
		fs.readFile(filepath, 'utf-8', function(err, txt) {
			CSV.parse(txt, {columns: true}, function(err, csv) {
				if (err) return cb(err);
				callback(err, csv)
			});		
		})
	},

    /**
     * Takes an array of objects specifying SAV files and returns a stacked object array
     * @param files Array [{ src: <filename}, attr1: <val>, attr2: <val> }, ...]     
     */
	stack_sav: function stack_sav(files, path,  callback) {
	    var self = this;
		async.map( 
			files,
			function(file, cb) {

				var filename = (path||"") + file.src;
				spssConvert.toCsv(filename, function(err, txt) {
					if (err) return cb(err);
				    CSV.parse(txt, {columns: true}, function(err, csv) {
						if (err) return cb(err);
						cb(err, csv)
					})
				})	 
		    }, 
		    function(err, rowsets) {
			  if (err) return callback(err);

			  rowsets.forEach(function(rows, idx) {
			  	rows.forEach(function(row) { 
			  		for (var k in files[idx]) row[k] = files[idx][k]
			  	})
			  })

			  var stacked = self.stack_rows(rowsets) 
			  callback(null, stacked)           
		    }
		);
	},

	export_csv: function(filepath, rows, callback) {
		  CSV.stringify(rows, {header: true}, function(err, csv) {
		  	fs.writeFile(filepath, csv, 'utf-8', callback)
		  })
	},

    /** 
     * Catenate an array of datasets
     */
	stack_rows: function(datasets) {
		return [].concat.apply([], datasets);
	},

	left_join: function(left_rows, right_rows, merge_keys, fields) {

		function get_key_value(row) {
			return JSON.stringify(merge_keys.map(function(key) { return row[key]; }))
		}

		var indexed = {};
		left_rows.forEach(function(left_row) {
			var row_key = get_key_value(left_row)
			indexed[row_key] = left_row;
		});


		right_rows.forEach(function(right_row, idx) {
 			var row_key = get_key_value(right_row)
 			var left_row = indexed[row_key];
 			if (left_row) {
 				fields.forEach(function(field) {
 					if (right_row[field] !== undefined) left_row[field] = right_row[field];
 				})
 			}
		})
 		return left_rows;
	}
}
