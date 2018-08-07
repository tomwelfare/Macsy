Overview
========

Getting started
---------------
To get started with the Macsy blackboard system, first instantiate a :class:`BlackboardAPI<macsy.api.BlackboardAPI>` object with your database settings.

Example:
    >>> from macsy.api import BlackboardAPI
    >>> settings = {'username' : 'user', 'password' : 'password', 'dbname' : 'database', 'dburl' : 'localhost:37017'}
    >>> api = BlackboardAPI(settings)

Once you have an instantiated :class:`BlackboardAPI<macsy.api.BlackboardAPI>`, you can query it for the available blackboards, and load the one you wish to use.

Example:
    >>> print(api.get_blackboard_names())
    ['ARTICLE', 'FEED', 'OUTLET', 'TWEET', 'MODULE', 'MODULE_RUN', 'URL']
    >>> blackboard = api.load_blackboard('ARTICLE')

You can now query the :class:`Blackboard<macsy.blackboards.Blackboard>` object for data.

Example:
    >>> for article in blackboard.find(max=10):
    >>> ... print(article['T'])

Types of Blackboard
-------------------

Blackboards can be one of two types: 
    1. Standard
    2. Date-based. 

:class:`DateBasedBlackboards<macsy.blackboards.DateBasedBlackboard>` hold data that are dated in some way, for example news articles have a publication date, or tweets have a date when they were written. This date is encoded in the id of the document via an :class:`ObjectId`.

To get the date of a document stored in a :class:`DateBasedBlackboard<macsy.blackboards.DateBasedBlackboard>`, you can use the :meth:`get_date()<macsy.blackboards.DateBasedBlackboard.get_date()>` method.

Standard :class:`Blackboards<macsy.blackboards.Blackboard>` are used to hold data that is static, for example news rss feeds which have a static URL. For these records, ids can be any type (most likely :class:`int` or :class:`ObjectId`).

Classes
-------
.. autosummary:: 
    macsy.api.BlackboardAPI
    macsy.blackboards.Blackboard
    macsy.blackboards.DateBasedBlackboard    
    macsy.cursors.BlackboardCursor