#-*- coding: utf-8 -*-

"""
Contains unit tests for the Python Parse REST API wrapper
"""

import os
import sys
import subprocess
import unittest
import datetime
import random


from core import ResourceRequestNotFound
from connection import register, ParseBatcher
from datatypes import GeoPoint, Object, Function, ParseField, ParseManyToManyField
from user import User
import query

try:
    import settings_local
except ImportError:
    sys.exit('You must create a settings_local.py file with APPLICATION_ID, ' \
                 'REST_API_KEY, MASTER_KEY variables set')

try:
    unicode = unicode
except NameError:
    # is python3
    unicode = str

register(
    getattr(settings_local, 'APPLICATION_ID'),
    getattr(settings_local, 'REST_API_KEY'),
    master_key=getattr(settings_local, 'MASTER_KEY')
    )

GLOBAL_JSON_TEXT = """{
    "applications": {
        "_default": {
            "link": "parseapi"
        },
        "parseapi": {
            "applicationId": "%s",
            "masterKey": "%s"
        }
    },
    "global": {
        "parseVersion": "1.1.16"
    }
}
"""


class Game(Object):
    pass


class GameScore(Object):
    pass


class City(Object):
    pass


class Review(Object):
    pass


class CollectedItem(Object):
    pass


class Foo(Object):
    pass
        

def get_order_number():
    return 5

current_order = 0
def get_sequential_order():
    global current_order
    current_order += 1
    return current_order



class Order(Object):
    total = ParseField(default=0)
    number = ParseField(default=get_order_number)
    customer = ParseField()

class SequentialOrder(Object):
    total = ParseField(default=0)
    number = ParseField(default=get_sequential_order)
    customer = ParseField()


class Address(Object):
    pass


class Customer(Object):
    addresses = ParseManyToManyField(Address)


class ParseFieldTestCase(unittest.TestCase):
    def test_can_set_existing_property(self):
        field = ParseField(default=1)
        self.assertEqual(field.default, 1)

    def test_cannot_set_nonexisting_property(self):
        self.assertRaises(AttributeError, ParseField, foo=1)


class DefaultValueObjectTestCase(unittest.TestCase):
    def tearDown(self):
        Order.Query.all().delete()

    def test_object_has_fields_with_default_value(self):
        order = Order()
        order.save()
        self.assertEqual(order.total, 0)

    def test_object_can_have_additional_fields(self):
        order = Order(number=1)
        order.save()
        self.assertEqual(order.total, 0)
        self.assertEqual(order.number, 1)

    def test_default_can_be_overridden(self):
        order = Order(total=15)
        order.save()
        self.assertEqual(order.total, 15)

    def test_callable_default_gets_value(self):
        order = Order()
        order.save()
        self.assertEqual(order.number, 5)

    def test_callable_default_called(self):
        order = SequentialOrder()
        order.save()
        self.assertEqual(order.number, 1)
        order = SequentialOrder()
        order.save()
        self.assertEqual(order.number, 2)


class TestManyToManyRelations(unittest.TestCase):
    def setUp(self):
        self.address1 = Address(
            number="1",
            street="Main St",
            city="Foobar",
            state="OR"
        )
        self.address1.save()
        self.address2 = Address(
            number="2",
            street="Main St",
            city="Foobar",
            state="OR"
        )
        self.address2.save()
        self.address3 = Address(
            number="3",
            street="Main St",
            city="Foobar",
            state="OR"
        )
        self.address3.save()

        self.customer1 = Customer(name="Jane Smith")
        self.customer1.save()
        self.customer2 = Customer(name="John Smith")
        self.customer2.save()

    def tearDown(self):
        qs = self.customer1.addresses.joint_class.Query.all()
        qs.delete()
        Address.Query.all().delete()
        Customer.Query.all().delete()

    def test_manytomany_field_returns_querymanager(self):
        self.assertIsInstance(self.customer1.addresses, query.QueryManager)
        self.assertTrue(issubclass(self.customer1.addresses.from_class, Customer))
        self.assertTrue(issubclass(self.customer1.addresses.to_class, Address))
        self.assertTrue(issubclass(self.customer1.addresses.model_class, Address))
        self.assertEqual(self.customer1.addresses.joint_class.__name__, 'CustomerAddresss')

    def test_add_related_object(self):
        self.customer1.addresses.add(self.address1)
        self.assertEqual(self.customer1.addresses.all()[0], self.address1)

    def test_add_related_objects(self):
        self.customer1.addresses.add(self.address1, self.address2)
        self.assertEqual(self.customer1.addresses.all().count(), 2)

    def test_clear_related_objects(self):
        self.customer1.addresses.add(self.address1, self.address2)
        self.customer1.addresses.clear()
        self.assertEqual(self.customer1.addresses.all().count(), 0)

    def test_clear_empty_related_objects(self):
        try:
            self.customer1.addresses.clear()
        except:
            self.fail('Failed to handle clearing empty m2m')

    def test_only_returns_related_objects(self):
        self.customer1.addresses.add(self.address1, self.address2)
        self.customer2.addresses.add(self.address3)
        self.assertEqual(self.customer2.addresses.all()[0], self.address3)

    def test_direct_set_m2m(self):
        self.customer1.addresses = [self.address1, self.address2]
        self.assertEqual(self.customer1.addresses.all().count(), 2)


class TestObject(unittest.TestCase):
    def setUp(self):
        self.score = GameScore(
            score=1337, player_name='John Doe', cheat_mode=False
            )
        self.sao_paulo = City(
            name='São Paulo', location=GeoPoint(-23.5, -46.6167)
            )

    def tearDown(self):
        city_name = getattr(self.sao_paulo, 'name', None)
        game_score = getattr(self.score, 'score', None)
        if city_name:
            for city in City.Query.filter(name=city_name):
                city.delete()

        if game_score:
            for score in GameScore.Query.filter(score=game_score):
                score.delete()

    def testCanInitialize(self):
        self.assert_(self.score.score == 1337, 'Could not set score')

    def testCanInstantiateParseType(self):
        self.assert_(self.sao_paulo.location.latitude == -23.5)

    def testCanSaveDates(self):
        now = datetime.datetime.now()
        self.score.last_played = now
        self.score.save()
        self.assert_(self.score.last_played == now, 'Could not save date')

    def testCanCreateNewObject(self):
        self.score.save()
        object_id = self.score.objectId

        self.assert_(object_id is not None, 'Can not create object')
        self.assert_(type(object_id) == unicode)
        self.assert_(type(self.score.createdAt) == datetime.datetime)
        self.assert_(GameScore.Query.filter(objectId=object_id).exists(),
                     'Can not create object')

    def testCanUpdateExistingObject(self):
        self.sao_paulo.save()
        self.sao_paulo.country = 'Brazil'
        self.sao_paulo.save()
        self.assert_(type(self.sao_paulo.updatedAt) == datetime.datetime)

        city = City.Query.get(name='São Paulo')
        self.assert_(city.country == 'Brazil', 'Could not update object')

    def testCanDeleteExistingObject(self):
        self.score.save()
        object_id = self.score.objectId
        self.score.delete()
        self.assert_(not GameScore.Query.filter(objectId=object_id).exists(),
                     'Failed to delete object %s on Parse ' % self.score)

    def testCanIncrementField(self):
        previous_score = self.score.score
        self.score.save()
        self.score.increment('score')
        self.assert_(GameScore.Query.filter(score=previous_score + 1).exists(),
                     'Failed to increment score on backend')

    def testAssociatedObject(self):
        """test saving and associating a different object"""
        collectedItem = CollectedItem(type="Sword", isAwesome=True)
        collectedItem.save()

        self.score.item = collectedItem
        self.score.save()

        # get the object, see if it has saved
        qs = GameScore.Query.get(objectId=self.score.objectId)
        self.assert_(isinstance(qs.item, Object),
                     "Associated CollectedItem is not an object")
        self.assert_(qs.item.type == "Sword",
                   "Associated CollectedItem does not have correct attributes")

    def testBatch(self):
        """test saving, updating and deleting objects in batches"""
        scores = [GameScore(score=s, player_name='Jane', cheat_mode=False)
                    for s in range(5)]
        batcher = ParseBatcher()
        batcher.batch_save(scores)
        self.assert_(GameScore.Query.filter(player_name='Jane').count() == 5,
                     "batch_save didn't create objects")
        self.assert_(all(s.objectId is not None for s in scores),
                     "batch_save didn't record object IDs")

        # test updating
        for s in scores:
            s.score += 10
        batcher.batch_save(scores)

        updated_scores = GameScore.Query.filter(player_name='Jane')
        self.assertEqual(sorted([s.score for s in updated_scores]),
                         range(10, 15), msg="batch_save didn't update objects")

        # test deletion
        batcher.batch_delete(scores)
        self.assert_(GameScore.Query.filter(player_name='Jane').count() == 0,
                     "batch_delete didn't delete objects")


    def test_empty_batch(self):
        scores = []
        batcher = ParseBatcher()
        try:
            batcher.batch_save(scores)
        except ValueError:
            self.fail('Batcher raised ValueError due to empty batch list')

class TestTypes(unittest.TestCase):
    def setUp(self):
        self.now = datetime.datetime.now()
        self.score = GameScore(
            score=1337, player_name='John Doe', cheat_mode=False,
            date_of_birth=self.now
            )
        self.sao_paulo = City(
            name='São Paulo', location=GeoPoint(-23.5, -46.6167)
            )

    def testCanConvertToNative(self):
        native_data = self.sao_paulo._to_native()
        self.assert_(type(native_data) is dict, 'Can not convert object to dict')

    def testCanConvertNestedLocation(self):
        native_sao_paulo = self.sao_paulo._to_native()
        location_dict = native_sao_paulo.get('location')

        self.assert_(type(location_dict) is dict,
                     'Expected dict after conversion. Got %s' % location_dict)
        self.assert_(location_dict.get('latitude') == -23.5,
                     'Can not serialize geopoint data')

    def testCanConvertDate(self):
        native_date = self.score._to_native().get('date_of_birth')
        self.assert_(type(native_date) is dict,
                     'Could not serialize date into dict')
        iso_date = native_date.get('iso')
        self.assert_(iso_date == self.now.isoformat(),
                     'Expected %s. Got %s' % (self.now.isoformat(), iso_date))


class TestQuery(unittest.TestCase):
    """Tests of an object's Queryset"""
    def setUp(self):
        """save a bunch of GameScore objects with varying scores"""
        # first delete any that exist
        for s in GameScore.Query.all():
            s.delete()
        for g in Game.Query.all():
            g.delete()

        self.game = Game(title="Candyland")
        self.game.save()

        self.scores = [
            GameScore(score=s, player_name='John Doe', game=self.game)
                        for s in range(1, 6)]
        for s in self.scores:
            s.save()

    def tearDown(self):
        '''delete all GameScore and Game objects'''
        for s in GameScore.Query.all():
            s.delete()
        self.game.delete()

    def test_create(self):
        gs = GameScore.Query.create(score=1, player_name="John Doe2", game=self.game)
        self.assertIsNotNone(gs.objectId)

    def test_delete_queryset(self):
        qs = GameScore.Query.all()
        qs.delete()
        self.assertEqual(GameScore.Query.all().count(), 0)

    def test_delete_empty_queryset(self):
        qs = GameScore.Query.all()
        qs.delete()
        try:
            self.assertEqual(GameScore.Query.all().count(), 0)
        except ValueError:
            self.fail('Deleting empty queryset raised ValueError')

    def test_indexing(self):
        qs = GameScore.Query.all()
        try:
            qs[0]
        except TypeError:
            self.fail('Indexing raised a TypeError')

    def testExists(self):
        """test the Queryset.exists() method"""
        for s in range(1, 6):
            self.assert_(GameScore.Query.filter(score=s).exists(),
                         "exists giving false negative")
        self.assert_(not GameScore.Query.filter(score=10).exists(),
                     "exists giving false positive")

    def testCanFilter(self):
        '''test the Queryset.filter() method'''
        for s in self.scores:
            qobj = GameScore.Query.filter(objectId=s.objectId).get()
            self.assert_(qobj.objectId == s.objectId,
                         "Getting object with .filter() failed")
            self.assert_(qobj.score == s.score,
                         "Getting object with .filter() failed")

        # test relational query with other Objects
        num_scores = GameScore.Query.filter(game=self.game).count()
        self.assert_(num_scores == len(self.scores),
                        "Relational query with .filter() failed")

    def testGetExceptions(self):
        '''test possible exceptions raised by Queryset.get() method'''
        self.assertRaises(query.QueryResourceDoesNotExist,
                          GameScore.Query.filter(score__gt=20).get)
        self.assertRaises(query.QueryResourceMultipleResultsReturned,
                          GameScore.Query.filter(score__gt=3).get)

    def testCanQueryDates(self):
        last_week = datetime.datetime.now() - datetime.timedelta(days=7)
        score = GameScore(name='test', last_played=last_week)
        score.save()
        self.assert_(GameScore.Query.filter(last_played=last_week).exists(),
                     'Could not run query with dates')

    def testComparisons(self):
        """test comparison operators- gt, gte, lt, lte, ne"""
        scores_gt_3 = list(GameScore.Query.filter(score__gt=3))
        self.assertEqual(len(scores_gt_3), 2)
        self.assert_(all([s.score > 3 for s in scores_gt_3]))

        scores_gte_3 = list(GameScore.Query.filter(score__gte=3))
        self.assertEqual(len(scores_gte_3), 3)
        self.assert_(all([s.score >= 3 for s in scores_gt_3]))

        scores_lt_4 = list(GameScore.Query.filter(score__lt=4))
        self.assertEqual(len(scores_lt_4), 3)
        self.assert_(all([s.score < 4 for s in scores_lt_4]))

        scores_lte_4 = list(GameScore.Query.filter(score__lte=4))
        self.assertEqual(len(scores_lte_4), 4)
        self.assert_(all([s.score <= 4 for s in scores_lte_4]))

        scores_ne_2 = list(GameScore.Query.filter(score__ne=2))
        self.assertEqual(len(scores_ne_2), 4)
        self.assert_(all([s.score != 2 for s in scores_ne_2]))

        # test chaining
        lt_4_gt_2 = list(GameScore.Query.filter(score__lt=4).filter(score__gt=2))
        self.assert_(len(lt_4_gt_2) == 1, 'chained lt+gt not working')
        self.assert_(lt_4_gt_2[0].score == 3, 'chained lt+gt not working')
        q = GameScore.Query.filter(score__gt=3, score__lt=3)
        self.assert_(not q.exists(), "chained lt+gt not working")

    def testOptions(self):
        """test three options- order, limit, and skip"""
        scores_ordered = list(GameScore.Query.all().order_by("score"))
        self.assertEqual([s.score for s in scores_ordered],
                         [1, 2, 3, 4, 5])

        scores_ordered_desc = list(GameScore.Query.all().order_by("score", descending=True))
        self.assertEqual([s.score for s in scores_ordered_desc],
                         [5, 4, 3, 2, 1])

        scores_limit_3 = list(GameScore.Query.all().limit(3))
        self.assert_(len(scores_limit_3) == 3, "Limit did not return 3 items")

        scores_skip_3 = list(GameScore.Query.all().skip(3))
        self.assert_(len(scores_skip_3) == 2, "Skip did not return 2 items")

    def testCanCompareDateInequality(self):
        today = datetime.datetime.today()
        tomorrow = today + datetime.timedelta(days=1)
        self.assert_(GameScore.Query.filter(createdAt__lte=tomorrow).count() == 5,
                     'Could not make inequality comparison with dates')


class TestFunction(unittest.TestCase):
    def setUp(self):
        '''create and deploy cloud functions'''
        original_dir = os.getcwd()

        cloud_function_dir = os.path.join(os.path.split(__file__)[0], 'cloudcode')
        os.chdir(cloud_function_dir)
        # write the config file
        with open("config/global.json", "w") as outf:
            outf.write(GLOBAL_JSON_TEXT % (settings_local.APPLICATION_ID,
                                           settings_local.MASTER_KEY))
        try:
            subprocess.call(["parse", "deploy"])
        except OSError as why:
            print("parse command line tool must be installed " \
                "(see https://www.parse.com/docs/cloud_code_guide)")
            self.skipTest(why)
        os.chdir(original_dir)

    def tearDown(self):
        for review in Review.Query.all():
            review.delete()

    def test_simple_functions(self):
        """test hello world and averageStars functions"""
        # test the hello function- takes no arguments

        hello_world_func = Function("hello")
        ret = hello_world_func()
        self.assertEqual(ret["result"], u"Hello world!")

        # Test the averageStars function- takes simple argument
        r1 = Review(movie="The Matrix", stars=5,
                    comment="Too bad they never made any sequels.")
        r1.save()
        r2 = Review(movie="The Matrix", stars=4, comment="It's OK.")
        r2.save()

        star_func = Function("averageStars")
        ret = star_func(movie="The Matrix")
        self.assertAlmostEqual(ret["result"], 4.5)


class TestUser(unittest.TestCase):
    USERNAME = "dhelmet@spaceballs.com"
    PASSWORD = "12345"

    def _get_user(self):
        try:
            user = User.signup(self.username, self.password)
        except:
            user = User.Query.get(username=self.username)
        return user

    def _destroy_user(self):
        user = self._get_logged_user()
        user and user.delete()

    def _get_logged_user(self):
        if User.Query.filter(username=self.username).exists():
            return User.login(self.username, self.password)
        else:
            return self._get_user()

    def setUp(self):
        self.username = TestUser.USERNAME
        self.password = TestUser.PASSWORD

        try:
            u = User.login(self.USERNAME, self.PASSWORD)
        except ResourceRequestNotFound:
            # if the user doesn't exist, that's fine
            return
        u.delete()

    def tearDown(self):
        self._destroy_user()

    def testCanSignUp(self):
        self._destroy_user()
        user = User.signup(self.username, self.password)
        self.assert_(user is not None)
        self.assert_(user.username == self.username)

    def testCanLogin(self):
        self._get_user()  # User should be created here.
        user = User.login(self.username, self.password)
        self.assert_(user.is_authenticated(), 'Login failed')

    def testCanUpdate(self):
        user = self._get_logged_user()
        phone_number = '555-5555'

        # add phone number and save
        user.phone = phone_number
        user.save()

        self.assert_(User.Query.filter(phone=phone_number).exists(),
                     'Failed to update user data. New info not on Parse')

    def test_user_login_uses_subclass(self):
        class CustomUser(User):
            @property
            def custom(self):
                return True

        CustomUser.signup(username="foo", password="bar")

        user = CustomUser.login('foo', 'bar')
        self.assertTrue(user.custom)


class TestUserPointer(unittest.TestCase):
    USERNAME = "dhelmet@spaceballs.com"
    PASSWORD = "12345"

    def setUp(self):
        self.username = TestUserPointer.USERNAME
        self.password = TestUserPointer.PASSWORD

        try:
            u = User.login(self.USERNAME, self.PASSWORD)
        except ResourceRequestNotFound:
            # if the user doesn't exist, that's fine
            return
        u.delete()

    def tearDown(self):
        Foo.Query.all().delete()
        self._destroy_user()

    def _get_user(self):
        try:
            user = User.signup(self.username, self.password)
        except:
            user = User.Query.get(username=self.username)
        return user

    def _destroy_user(self):
        user = self._get_logged_user()
        user and user.delete()

    def _get_logged_user(self):
        if User.Query.filter(username=self.username).exists():
            return User.login(self.username, self.password)
        else:
            return self._get_user()

    def test_user_pointer(self):
        f = Foo(user=self._get_user())
        f.save()
        new_f = Foo.Query.get(objectId=f.objectId)
        self.assertEqual(new_f.user, f.user)

if __name__ == "__main__":
    # command line
    unittest.main()
