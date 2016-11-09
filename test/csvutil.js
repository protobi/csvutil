var assert = require('assert');
var CSVUtil = require('..');

describe("CSVUtil", function() {
	
	it ("Joins arrays", function(done) {
		var left = [
			{"a": "1", "b":"apple"},
			{"a": "2", "b":"berry"},
			{"a": "3", "b":"cherry"},
			{"a": "4", "b":"date"},
			{"a": "5", "b":"elderberry"},
		]
		var right = [
			{ "a": "1", "color": "red"},
			{ "a": "2", "color": "blue"},
			{ "a": "5", "color": "blue"},
			{ "a": "6", "color": "green"},
		]
		var expected = [ 
			{ a: '1', b: 'apple', color: 'red' },
			{ a: '2', b: 'berry', color: 'blue' },
			{ a: '3', b: 'cherry' },
			{ a: '4', b: 'date' },
			{ a: '5', b: 'elderberry', color: 'blue' } 
        ]

		var merged = CSVUtil.left_join(left, right, ["a"], ["color"])
	    assert.deepEqual(merged, expected)
	    done()
	})
})
