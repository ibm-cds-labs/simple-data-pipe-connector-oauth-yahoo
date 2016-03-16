//-------------------------------------------------------------------------------
// Copyright IBM Corp. 2015
//
// Licensed under the Apache License, Version 2.0 (the 'License');
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an 'AS IS' BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//-------------------------------------------------------------------------------

'use strict';

var util = require('util');

var pipesSDK = require('simple-data-pipe-sdk');
var connectorExt = pipesSDK.connectorExt;

var bluemixHelperConfig = require('bluemix-helper-config');
var global = bluemixHelperConfig.global;

// This connector uses the (OAuth1.0) passport strategy module (http://passportjs.org/) for yahoo.
var dataSourcePassportStrategy = require('passport-yahoo-oauth2').Strategy; 

// References: 
//  OAUTH 1.0: https://developer.yahoo.com/oauth/guide/oauth-auth-flow.html 
//  OAUTH 2.0: https://developer.yahoo.com/oauth2/guide/flows_authcode/
var OAuth = require('oauth');

/**
 * Simple Data Pipe connector boilerplate for Yahoo, using OAuth1 authentication. 
 * Build your own connector by following the TODO instructions
 * TODO: rename the class
 */
function oAuthYahooConnector(){

	 /* 
	  * Customization is mandatory
	  */

	// TODO: 
	//   Replace 'Yahoo OAuth Data Source' with the desired display name of the data source (e.g. 'Yahoo fantasy sports') from which data will be loaded
	var connectorInfo = {
						  id: require('../package.json').simple_data_pipe.name,			// derive internal connector ID from package.json
						  name: 'Yahoo OAuth1.0 Data Source'							// TODO; change connector display name
						};

	// TODO: customize options						
	var connectorOptions = {
					  		recreateTargetDb: true, // if set (default: false) all data currently stored in the staging database is removed prior to data load
					  		useCustomTables: true   // keep true (default: false)
						   };						

	// Call constructor from super class; 
	connectorExt.call(this, 
					 	connectorInfo.id, 			
					 	connectorInfo.name, 
					 	connectorOptions	  
					 );	

	// writes to the application's global log file
	var globalLog = this.globalLog;

	// simple wrapper to make Yahoo API calls using OAuth
	var yahooOAuthConsumer = null;

	/*
	 * ---------------------------------------------------------------------------------------
	 * Override Passport-specific connector methods:
	 *  - getPassportAuthorizationParams
	 *  - getPassportStrategy
	 *  - passportAuthCallbackPostProcessing
	 * ---------------------------------------------------------------------------------------
	 */

	/**
	 * The Yahoo OAuth1 API does not require extra authorization parameters
	 * @override
	 * @returns {} 
	 */
	this.getPassportAuthorizationParams = function() {
       return {};
	}; // getPassportAuthorizationParams

	/**
	 * Returns a fully configured Passport strategy for yahoo. The passport verify
	 * callback adds two properties to the profile: oauth_access_token and oauth_token_secret.
	 * @override
	 * @returns {Object} Passport strategy for yahoo.
	 * @returns {Object} profile - user profile returned by reddit
	 * @returns {string} profile.oauth_access_token
	 * @returns {string} profile.oauth_token_secret
	 */
	this.getPassportStrategy = function(pipe) {

		globalLog.info('Creating Passport strategy ...');	

		return new dataSourcePassportStrategy({
			consumerKey: pipe.clientId,											 // mandatory; oAuth client id; do not change
	        consumerSecret: pipe.clientSecret,									 // mandatory; oAuth client secret;do not change
	        callbackURL: global.getHostUrl() + '/authCallback'		 			 // mandatory; oAuth callback; do not change
		  },
		  function(token, tokensecret, profile, done) {					 

			  process.nextTick(function () {

			  	// attach the obtained access token to the user profile
		        profile.oauth_access_token = token; 

			  	// attach the obtained token secret to the user profile		        
		        profile.oauth_token_secret = tokensecret; 

		        // return the augmented profile
			    return done(null, profile);
			  });
		  }
		);
	}; // getPassportStrategy

	/**
	 * TODO Change the method as needed
	 * Attach OAuth tokens and list of available data sets to data pipe configuration.
	 * @param {Object} profile - the output returned by the passport verify callback
	 * @param {pipe} pipe - data pipe configuration, for which OAuth processing has been completed
	 * @param callback(err, pipe ) error information in case of a problem or the updated pipe
	 */
	this.passportAuthCallbackPostProcessing = function( profile, pipe, callback ){
		
		// use globalLog to write to the application log

		if((!profile) || (! profile.oauth_access_token) || (! profile.oauth_token_secret)) {
			globalLog.error('Internal application error: OAuth parameter is missing in passportAuthCallbackPostProcessing');
			return callback('Internal application error: OAuth parameter is missing.'); 
		}

		if(!pipe) {
			globalLog.error('Internal application error: data pipe configuration parameter is missing in passportAuthCallbackPostProcessing');
			return callback('Internal application error: data pipe configuration parameter is missing.'); 
		}

        // Attach the token(s) and other relevant information from the profile to the pipe configuration.
        // Use this information in the connector code to access the data source

		pipe.oAuth = { 
						accessToken : profile.oauth_access_token, 
						refreshToken: profile.oauth_token_secret 
					};

		// invalidate the consumer 			
		yahooOAuthConsumer = null;			

		// Fetch list of data sets that the user can choose from; the list is displayed in the Web UI in the "Filter Data" panel.
        // Attach data set list to the pipe configuration
        this.getYahooDataSetList(pipe, function (err, pipe){
		//this.getYahooDataSetList(pipe, function (err, pipe){
		    if(err) {
		    	globalLog.error('The Yahoo data set list could not be created: ' + err);
		    }		
			return callback(err, pipe);
		});

	}; // passportAuthCallbackPostProcessing

	/**
	  * Fetches list of Yahoo data set(s) that the user can choose from. At least one data set must be returned or the
	  * user will not be able to run the data pipe.
	  * @param {Object} pipe - data pipe configuration
	  * @param {callback} done - callback(err, updated_data_pipe_confguration) to be invoked when processing is done 
	  */
	this.getYahooDataSetList = function(pipe, done) {

		// List of yahoo data sets a user can choose from; at least one data set entry needs to be provided.
		var dataSets = [];

		// Initialize Yahoo API interface (the OAuth consumer)
		if(! yahooOAuthConsumer) {
			this.initializeOAuthConsumer(pipe);
		}
		
		// TODO:
		// Add Yahoo data sets that the user can choose from. At least one data set entry must be defined.
		// Add data sets statically
		dataSets.push({label:'Static Yahoo data set 1', name:'yahoo_ds_1'});

		// .. or dynamically, for example, based on results returned by a yahoo query.
		// Run some Yahoo query, extract relevant information and add to dataSets:
		dataSets.push({label:'Dynamic Yahoo data set', name:'yahoo_ds_2'});
		// Refer to method fetchRecords for an example on how to call the Yahoo API.

		// TODO: If you want to provide the user with the option to load all data sets concurrently, define a single data set that 
		// contains only property 'labelPlural', with a custom display label: 
		// dataSets.push({labelPlural:'All Yahoo data sets'});			

		// In the Simple Data Pipe UI the user gets to choose from: 
		//  -> All data sets 
		//  -> Static Yahoo data set 1
		//  -> Dynamic Yahoo data set
		//  -> ...

		// sort list by display label and attach the information to the data pipe configuration; if present, the ALL_DATA option should be displayed first
		pipe.tables = dataSets.sort(function (dataSet1, dataSet2) {
																if(! dataSet1.label)	{ // ALL_DATA (only property labelPlural is defined)
																	return -1;
																}

																if(! dataSet2.label) {// ALL_DATA (only property labelPlural is defined)
																	return 1;
																}

																return dataSet1.label.localeCompare(dataSet2.label);
															   });
		// Invoke callback and pass along the updated data pipe configuration, which now includes a list of Yahoo data sets the user
		// gets to choose from.
		return done(null, pipe);
	}; 

	/*
	 * ---------------------------------------------------------------------------------------
	 * Override general connector methods:
	 *  - doConnectStep: verify that OAuth information is still valid
	 *  - fetchRecords:  load data from data source
	 * ---------------------------------------------------------------------------------------
	 */

	/**
	* Customization might be required.
	* During data pipe runs, this method is invoked first. Add custom code as required, for example to verify that the 
	* OAuth token has not expired.
	* @param {callback} done - callback(err) that must be called when the connection is established
	* @param {Object} pipeRunStep
	* @param {Object} pipeRunStats
	* @param {Object} pipeRunLog - a dedicated logger instance that is only available during data pipe runs
	* @param {Object} pipe - data pipe configuration
	* @param {Object} pipeRunner
	*/
	this.doConnectStep = function( done, pipeRunStep, pipeRunStats, pipeRunLog, pipe, pipeRunner ){

		// Use the Yahoo OAuth information in pipe.oAuth in your API requests. 

		// Bunyan logging - https://github.com/trentm/node-bunyan
		// The log file is attached to the pipe run document, which is stored in the Cloudant repository database named pipe_db.
		// To enable debug logging, set environment variable DEBUG to '*' or to 'sdp-pipe-run' (without the quotes).
		pipeRunLog.info('Verifying OAuth connectivity for data pipe ' + pipe._id);

		// Initialize Yahoo API interface (the OAuth consumer)
		if(! yahooOAuthConsumer) {
			this.initializeOAuthConsumer(pipe);
		}

		// Invoke done callback to indicate that connectivity to the data source has been validated
		// Parameters:
		//  done()                                      // no parameter; processing completed successfully. no status message text is displayed to the end user in the monitoring view
		//  done({infoStatus: 'informational message'}) // processing completed successfully. the value of the property infoStatus is displayed to the end user in the monitoring view
		//  done({errorStatus: 'error message'})        // a fatal error was encountered during processing. the value of the property infoStatus is displayed to the end user in the monitoring view
		//  done('error message')                       // deprecated; a fatal error was encountered during processing. the message is displayed to the end user in the monitoring view
		if(yahooOAuthConsumer) {
			return done();
		}
		else {
			pipeRunLog.error('The Yahoo API interface could not be initialized. Aborting data pipe run because no API calls can be processed.');
			return done({errorStatus: 'The Yahoo API interface could not be initialized.'});
		}
			
	}; // doConnectStep

	/*
	 * Customization is mandatory!
	 * Implement the code logic to fetch data from the source, optionally enrich it and store it in Cloudant.
	 * @param {Object} dataSet - dataSet.name contains the data set name that was (directly or indirectly) selected by the user
	 * @param {callback} done - callback function to be invoked after processing is complete (or a fatal error has been encountered)
	 * @param {Object} pipe - data pipe configuration
	 * @param {Object} pipeRunLog - a dedicated logger instance that is only available during data pipe runs
	 */
	this.fetchRecords = function( dataSet, pushRecordFn, done, pipeRunStep, pipeRunStats, pipeRunLog, pipe, pipeRunner ){

		// The data set is typically selected by the user in the "Filter Data" panel during the pipe configuration step
		// dataSet: {name: 'data set name', label: 'data set label'}. However, if you enabled the ALL option and it was selected, 
		// the fetchRecords function is invoked asynchronously once for each data set. See getYahooDataSetList

		// Bunyan logging - https://github.com/trentm/node-bunyan
		// The log file is attached to the pipe run document, which is stored in the Cloudant repository database named pipe_db.
		// To enable debug logging, set environment variable DEBUG to '*' or to 'sdp-pipe-run' (without the quotes).
		pipeRunLog.info('Data pipe ' + pipe._id + ' is fetching data for data set ' + dataSet.name + ' from yahoo.');

		// Use the Yahoo OAuth information in pipe.oAuth in your API requests. See passportAuthCallbackPostProcessing


	    // TODO: fetch data from yahoo
	    // ... (sample only)
		var yahooUrl = 'https://query.yahooapis.com/v1/yql?q=select%20*%20from%20fantasysports.teams.stats%20where%20team_key%3D%22113.l.231619.t.11%22&format=json&diagnostics=true&callback=';

		pipeRunLog.info('Calling Yahoo API ...');	
		pipeRunLog.debug('API: ' + yahooUrl);

		// yahooOAuthConsumer was initialized in doConnectStep, which is always processed first during a data pipe run
		yahooOAuthConsumer.get(yahooUrl,
					  	       pipe.oAuth.accessToken,
					  	       pipe.oAuth.refreshToken, 
					  	       function(err, data, result) {
					  	        	if(err) {
					  	        		pipeRunLog.error('Call to Yahoo API ' + yahooUrl + ' failed: ' + util.inspect(err,3));
					  	        		return done('Call to ' + yahooUrl + ' failed: ' + JSON.parse(err.data).error.description);
					  	        	}

					  	        	pipeRunLog.debug('API response - data : ' + util.inspect(data,4));
					  	        	// pipeRunLog.debug('API response - result : ' + util.inspect(result,4));

								    // TODO: optionally subset/enrich the data
								    // ...

								    // Save query results in Cloudant
								    var record = {
								    				results: JSON.parse(data).query.results
								    				// ...
								    			 };

									// Method accepts a single record or an array of records as parameter	    			 
								    pushRecordFn(record);

									// Invoke done callback to indicate that data set dataSet has been processed. 
									// Parameters:
									//  done()                                      // no parameter; processing completed successfully. no status message text is displayed to the end user in the monitoring view
									//  done({infoStatus: 'informational message'}) // processing completed successfully. the value of the property infoStatus is displayed to the end user in the monitoring view
									//  done({errorStatus: 'error message'})        // a fatal error was encountered during processing. the value of the property infoStatus is displayed to the end user in the monitoring view
									//  done('error message')                       // deprecated; a fatal error was encountered during processing. the message is displayed to the end user in the monitoring view
									return done();
		});

	}; // fetchRecords

	/*
	 * Customization is not needed.
	 */
	this.getTablePrefix = function(){
		// The prefix is used to generate names for the Cloudant staging databases that store your data. 
		// The recommended value is the connector ID to assure uniqueness.
		return connectorInfo.id;
	};


	/*
	 *  --------------------------------------------------------------------------------------------------------------------
	 *   Internal helper methods
	 *  --------------------------------------------------------------------------------------------------------------------
	 */

    /**
     * Instantiates OAuth consumer that can be used for Yahoo API calls
     * @param {Object} pipe - data pipe configuration
     */
	this.initializeOAuthConsumer  = function(pipe) {

		if(pipe) {
			yahooOAuthConsumer = new OAuth.OAuth(
							 				    'https://api.login.yahoo.com/oauth/v2/get_request_token',
											    'https://api.login.yahoo.com/oauth/v2/get_token',
											    pipe.clientId,
											    pipe.clientSecret,
											    '1.0',
											    null,
											    'HMAC-SHA1'
			);
		}
		else {
			yahooOAuthConsumer = null;
		}
	};

} // function oAuthYahooConnector

//Extend event Emitter
util.inherits(oAuthYahooConnector, connectorExt);

module.exports = new oAuthYahooConnector(); 