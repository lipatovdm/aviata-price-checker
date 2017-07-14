from app import app
from pymongo import MongoClient
from bson import BSON
from bson import json_util
import pprint
import json
import datetime
from flask import jsonify, request, render_template
from bson.son import SON


client = MongoClient('aviata.kz', 27017)
db = client.air
air_search = db['search_stat']

@app.route('/')
@app.route('/index')
def index():
	return render_template('index.html')

@app.route('/days/')
def days():
	route = request.args['flight'].split('-')

	date_from = datetime.datetime(2017, 7, 1)
	date_to = datetime.datetime(2017, 7, 14)
	city_from = route[0].upper()
	city_to = route[1].upper()
	flight_type = "oneway"
	is_domestic = True
	cabin_class = "Economy"
	

	pipeline = [
	{"$match": {
		"domestic": is_domestic,
		"created": {"$gte": date_from, "$lte": date_to},
		"routes": [city_from, city_to],
		"flight_type" : flight_type,
		"cabin_class": cabin_class,
		"adults": 1,
		"flight_date_to": {"$gte": date_from, "$lte": datetime.datetime(date_to.year, date_to.month, date_to.day+1)}
		}},
	{"$project": {
			"_id": {
				"year": {"$year": "$created"},
				"month": {"$month": "$created"},
				"day": {"$dayOfMonth": "$created"},
			},
			"airline": "$cheapest.carrier",
			"price": "$cheapest.price",
			"route": "$routes",
			"flight_type": "$flight_type",
			"adults_num": "$adults",
			"flight_date": "$flight_date_to",
			"created": "$created",
	}
	},
	{"$group": {
		"_id": {"date": "$_id", "airline": "$airline"},
		"avg_price": {"$avg": "$price"},
		"min_price": {"$min": "$price"},
		"max_price": {"$max": "$price"},
		"flight_date": {"$push": "$flight_date"},
		"show_count": {"$sum": 1},
		"flight_type": {"$first": "$flight_type"},
		"route" : {"$first": "$route"},
		"date_from": {"$min": "$created"},
		"date_to": {"$max": "$created"}

	}},
	{"$group": {
		"_id": "$_id.date",
		"result": {"$push": {"airline": "$_id.airline", "min_price": "$min_price", "max_price": "$max_price", "avg_price": "$avg_price"}},
		"route": {"$first": "$route"}
		
	}},
	{"$sort": SON([("_id.month", 1), ("_id.day", 1)])}]

	result = []

	for item in air_search.aggregate(pipeline):
		result.append(item)
	
	return render_template('airline.html', data=result)
	# return json.dumps(result, ensure_ascii=False)


	# return "OK"

@app.route('/airline/<carrier>')
def airline(carrier):
	# route = request.args['flight'].split('-')

	date_from = datetime.datetime(2017, 7, 1)
	date_to = datetime.datetime(2017, 7, 14)
	# city_from = route[0].upper()
	# city_to = route[1].upper()
	flight_type = "oneway"
	is_domestic = True
	cabin_class = "Economy"
	carrier = carrier.upper()

	pipeline = [
	{"$match": {
		"domestic": is_domestic,
		"created": {"$gt": date_from, "$lt": date_to},
		# "routes": [city_from, city_to],
		"flight_type" : flight_type,
		"cabin_class": cabin_class,
		"cheapest.carrier": carrier,
		"adults": {"$eq": 1}
	}},
	{"$project": {
			"_id": {
				"year": {"$year": "$created"},
				"month": {"$month": "$created"},
				"day": {"$dayOfMonth": "$created"},
			},
			"airline": "$cheapest.carrier",
			"price": "$cheapest.price",
	}},
	{"$group": {
		"_id": "$_id",
		"avg_price": {"$avg": "$price"},
		"min_price": {"$min": "$price"},
		"max_price": {"$max": "$price"},

	}},
	{"$sort": SON([("_id.month", 1), ("_id.day", 1)])}]

	result = []

	for item in air_search.aggregate(pipeline):
		result.append(item)
	
	return render_template('carrier.html', data=result)

@app.route('/hours')
def hours():
	test_data = air_search.find_one({'cheapest.carrier': "KC"})

	date_from = datetime.datetime(2017, 7, 1)
	date_to = datetime.datetime(2017, 7, 2)
	city_from = "ALA"
	city_to = "TSE"
	flight_type = "oneway"
	is_domestic = True
	cabin_class = "Economy"

	pipeline = [
	{"$match": {
		"domestic": is_domestic,
		"created": {"$gte": date_from, "$lte": date_to},
		"routes": [city_from, city_to],
		"flight_type" : flight_type,
		"cabin_class": cabin_class,
		"adults": 1,
		"flight_date_to": {"$gte": date_from, "$lte": datetime.datetime(date_to.year, date_to.month, date_to.day+1)}
		}},
	{"$project": {
			"_id": {
				"year": {"$year": "$created"},
				"month": {"$month": "$created"},
				"day": {"$dayOfMonth": "$created"},
				"hour": {"$hour": "$created"},
			},
			"airline": "$cheapest.carrier",
			"price": "$cheapest.price",
			"route": "$routes",
			"flight_type": "$flight_type",
			"adults_num": "$adults",
			"flight_date": "$flight_date_to"
	}
	},
	{"$group": {
		"_id": {"date": "$_id", "airline": "$airline"},
		"avg_price": {"$avg": "$price"},
		"min_price": {"$min": "$price"},
		"max_price": {"$max": "$price"},
		"flight_date": {"$push": "$flight_date"},
		"show_count": {"$sum": 1},
		"flight_type": {"$first": "$flight_type"},
		"route" : {"$first": "$route"}

	}},
	{"$group": {
		"_id": "$_id.date",
		"result": {"$push": {"airline": "$_id.airline", "min_price": "$min_price", "max_price": "$max_price", "avg_price": "$avg_price"}} 
	}},
	{"$sort": SON([("_id.date.month", 1), ("_id.date.day", 1)])}]

	result = []

	for item in air_search.aggregate(pipeline):
		result.append(item)
	
	return render_template('index.html', data=result)
	# return json.dumps(result, ensure_ascii=False)


	# return "OK"

@app.route('/all_dom')
def all_dom():
	date_from = datetime.datetime(2017, 7, 1)
	date_to = datetime.datetime(2017, 7, 14)
	flight_type = "oneway"
	is_domestic = True
	cabin_class = "Economy"

	pipeline = [
	{"$match": {
		"domestic": is_domestic,
		"created": {"$gte": date_from, "$lte": date_to},
		"flight_type" : flight_type,
		"cabin_class": cabin_class,
		"$nor": [{"routes": ["ALA", "TSE"]}, {"routes": ["TSE", "ALA"]}],
		"adults": 1
		}},
	{"$project": {
			"_id": {
				"year": {"$year": "$created"},
				"month": {"$month": "$created"},
				"day": {"$dayOfMonth": "$created"}
			},
			"airline": "$cheapest.carrier",
			"price": "$cheapest.price",
			"flight_type": "$flight_type",
	}
	},
	{"$group": {
		"_id": {"date": "$_id"},
		"avg_price": {"$avg": "$price"},
		"min_price": {"$min": "$price"},
		"max_price": {"$max": "$price"},

	}},
	{"$sort": SON([("_id.date.month", 1), ("_id.date.day", 1)])}]

	result = []

	for item in air_search.aggregate(pipeline):
		result.append(item)
	
	return render_template('routes.html', data=result)

@app.route('/route')
def route():
	date_from = datetime.datetime(2017, 5, 1)
	date_to = datetime.datetime(2017, 7, 10)
	flight_type = "oneway"
	is_domestic = True
	cabin_class = "Economy"
	route = request.args['flight'].split('-')


	pipeline = [
	{"$match": {
		"domestic": is_domestic,
		"created": {"$gte": date_from, "$lte": date_to},
		"flight_type" : flight_type,
		"cabin_class": cabin_class,
		"routes": [route[0].upper(), route[1].upper()],
		"adults": 1
		}},
	{"$project": {
			"_id": {
				"year": {"$year": "$created"},
				"month": {"$month": "$created"},
				"day": {"$dayOfMonth": "$created"}
			},
			"airline": "$cheapest.carrier",
			"price": "$cheapest.price",
			"flight_type": "$flight_type",
	}
	},
	{"$group": {
		"_id": {"date": "$_id"},
		"avg_price": {"$avg": "$price"},
		"min_price": {"$min": "$price"},
		"max_price": {"$max": "$price"},

	}},
	{"$sort": SON([("_id.date.month", 1), ("_id.date.day", 1)])}]

	result = []

	for item in air_search.aggregate(pipeline):
		result.append(item)
	
	return render_template('routes.html', data=result)

@app.route('/route_search_stats')
def route_count_stats():
	route = request.args['flight'].split('-')

	print(route)


	test_data = air_search.find_one({'cheapest.carrier': "KC"})

	date_from = datetime.datetime(2017, 5, 1)
	date_to = datetime.datetime(2017, 7, 10)
	city_from = route[0].upper()
	city_to = route[1].upper()
	flight_type = "oneway"
	is_domestic = True
	cabin_class = "Economy"

	print(route[0].upper())

	pipeline = [
	{"$match": {
		"domestic": is_domestic,
		"created": {"$gte": date_from, "$lte": date_to},
		"routes": ["ALA", "TSE"],
		"flight_type" : flight_type,
		"cabin_class": cabin_class,
		"adults": 1,
		}},
	{"$project": {
			"_id": {
				"year": {"$year": "$created"},
				"month": {"$month": "$created"},
				"day": {"$dayOfMonth": "$created"}
			},
			"count": 1
	}},
	{"$group": {
		"_id": {"date": "$_id"},
		"searches": {"$sum": 1}

	}},{"$sort": SON([("_id.date.month", 1), ("_id.date.day", 1)])}]

	result = []

	for item in air_search.aggregate(pipeline):
		result.append(item)
	
	return render_template('route_stat.html', data=result)