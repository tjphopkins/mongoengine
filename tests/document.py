import pickle
import pymongo
import bson
import unittest
import warnings

from datetime import datetime

from fixtures import Base, Mixin, PickleEmbedded, PickleTest

from mongoengine import *
from mongoengine.base import NotRegistered, InvalidDocumentError
from mongoengine.queryset import InvalidQueryError
from mongoengine.connection import get_db, register_db


class DocumentTest(unittest.TestCase):

    def setUp(self):
        connect()
        register_db('mongoenginetest')
        self.db = get_db()

        class Person(Document):
            name = StringField()
            age = IntField()

            meta = {'allow_inheritance': True}

        self.Person = Person

    def tearDown(self):
        self.Person.drop_collection()

    def test_future_warning(self):
        """Add FutureWarning for future allow_inhertiance default change.
        """

        with warnings.catch_warnings(True) as errors:

            class SimpleBase(Document):
                a = IntField()

            class InheritedClass(SimpleBase):
                b = IntField()

            InheritedClass()
            self.assertEquals(len(errors), 1)
            warning = errors[0]
            self.assertEquals(FutureWarning, warning.category)
            self.assertTrue("InheritedClass" in warning.message.message)

    def test_drop_collection(self):
        """Ensure that the collection may be dropped from the database.
        """
        self.Person(name='Test').save()

        collection = self.Person._get_collection_name()
        self.assertTrue(collection in self.db.collection_names())

        self.Person.drop_collection()
        self.assertFalse(collection in self.db.collection_names())

    def test_queryset_resurrects_dropped_collection(self):

        list(self.Person.objects())
        self.Person.drop_collection()

        self.assertEqual([], list(self.Person.objects()))

        class Actor(self.Person):
            pass

        # Ensure works correctly with inhertited classes
        list(Actor.objects())
        self.Person.drop_collection()
        self.assertEqual([], list(Actor.objects()))

    def test_definition(self):
        """Ensure that document may be defined using fields.
        """
        name_field = StringField()
        age_field = IntField()

        class Person(Document):
            name = name_field
            age = age_field
            non_field = True

        self.assertEqual(Person._fields['name'], name_field)
        self.assertEqual(Person._fields['age'], age_field)
        self.assertFalse('non_field' in Person._fields)
        self.assertTrue('id' in Person._fields)
        # Test iteration over fields
        fields = list(Person())
        self.assertTrue('name' in fields and 'age' in fields)
        # Ensure Document isn't treated like an actual document
        self.assertFalse(hasattr(Document, '_fields'))

    def test_collection_naming(self):
        """Ensure that a collection with a specified name may be used.
        """

        class DefaultNamingTest(Document):
            pass
        self.assertEquals('default_naming_test', DefaultNamingTest._get_collection_name())

        class CustomNamingTest(Document):
            meta = {'collection': 'pimp_my_collection'}

        self.assertEquals('pimp_my_collection', CustomNamingTest._get_collection_name())

        class DynamicNamingTest(Document):
            meta = {'collection': lambda c: "DYNAMO"}
        self.assertEquals('DYNAMO', DynamicNamingTest._get_collection_name())

        # Use Abstract class to handle backwards compatibility
        class BaseDocument(Document):
            meta = {
                'abstract': True,
                'collection': lambda c: c.__name__.lower()
            }

        class OldNamingConvention(BaseDocument):
            pass
        self.assertEquals('oldnamingconvention', OldNamingConvention._get_collection_name())

        class InheritedAbstractNamingTest(BaseDocument):
            meta = {'collection': 'wibble'}
        self.assertEquals('wibble', InheritedAbstractNamingTest._get_collection_name())

        with warnings.catch_warnings(record=True) as w:
            # Cause all warnings to always be triggered.
            warnings.simplefilter("always")

            class NonAbstractBase(Document):
                pass

            class InheritedDocumentFailTest(NonAbstractBase):
                meta = {'collection': 'fail'}

            self.assertTrue(issubclass(w[0].category, SyntaxWarning))
            self.assertEquals('non_abstract_base', InheritedDocumentFailTest._get_collection_name())

        # Mixin tests
        class BaseMixin(object):
            meta = {
                'collection': lambda c: c.__name__.lower()
            }

        class OldMixinNamingConvention(Document, BaseMixin):
            pass
        self.assertEquals('oldmixinnamingconvention', OldMixinNamingConvention._get_collection_name())

        class BaseMixin(object):
            meta = {
                'collection': lambda c: c.__name__.lower()
            }

        class BaseDocument(Document, BaseMixin):
            meta = {'allow_inheritance': True}

        class MyDocument(BaseDocument):
            pass

        self.assertEquals('basedocument', MyDocument._get_collection_name())

    def test_get_superclasses(self):
        """Ensure that the correct list of superclasses is assembled.
        """
        class Animal(Document):
            meta = {'allow_inheritance': True}
        class Fish(Animal): pass
        class Mammal(Animal): pass
        class Human(Mammal): pass
        class Dog(Mammal): pass

        mammal_superclasses = {'Animal': Animal}
        self.assertEqual(Mammal._superclasses, mammal_superclasses)

        dog_superclasses = {
            'Animal': Animal,
            'Animal.Mammal': Mammal,
        }
        self.assertEqual(Dog._superclasses, dog_superclasses)


    def test_external_superclasses(self):
        """Ensure that the correct list of sub and super classes is assembled.
        when importing part of the model
        """
        class Animal(Base): pass
        class Fish(Animal): pass
        class Mammal(Animal): pass
        class Human(Mammal): pass
        class Dog(Mammal): pass

        mammal_superclasses = {'Base': Base, 'Base.Animal': Animal}
        self.assertEqual(Mammal._superclasses, mammal_superclasses)

        dog_superclasses = {
            'Base': Base,
            'Base.Animal': Animal,
            'Base.Animal.Mammal': Mammal,
        }
        self.assertEqual(Dog._superclasses, dog_superclasses)

        Base.drop_collection()

        h = Human()
        h.save()

        self.assertEquals(Human.objects.count(), 1)
        self.assertEquals(Mammal.objects.count(), 1)
        self.assertEquals(Animal.objects.count(), 1)
        self.assertEquals(Base.objects.count(), 1)
        Base.drop_collection()

    def test_polymorphic_queries(self):
        """Ensure that the correct subclasses are returned from a query.
        Note: a parent class will fetch child classes but an intermedia class
        will not fetch its child classes.
        """
        class Animal(Document):
            meta = {'allow_inheritance': True}
        class Fish(Animal): pass
        class Mammal(Animal): pass
        class Human(Mammal): pass
        class Dog(Mammal): pass

        Animal.drop_collection()

        Animal().save()
        Fish().save()
        Mammal().save()
        Human().save()
        Dog().save()

        classes = [obj.__class__ for obj in Animal.objects]
        self.assertEqual(classes, [Animal, Fish, Mammal, Human, Dog])

        classes = [obj.__class__ for obj in Mammal.objects]
        self.assertEqual(classes, [Mammal, Human, Dog])

        classes = [obj.__class__ for obj in Human.objects]
        self.assertEqual(classes, [Human])

        Animal.drop_collection()

    def test_reference_inheritance(self):
        class Stats(Document):
            created = DateTimeField(default=datetime.now)

            meta = {'allow_inheritance': False}

        class CompareStats(Document):
            generated = DateTimeField(default=datetime.now)
            stats = ListField(ReferenceField(Stats))

        Stats.drop_collection()
        CompareStats.drop_collection()

        list_stats = []

        for i in xrange(10):
            s = Stats()
            s.save()
            list_stats.append(s)

        cmp_stats = CompareStats(stats=list_stats)
        cmp_stats.save()

        self.assertEqual(
            [s.id for s in list_stats],
            [s.id for s in CompareStats.objects.first().stats])

    def test_inheritance(self):
        """Ensure that document may inherit fields from a superclass document.
        """
        class Employee(self.Person):
            salary = IntField()

        self.assertTrue('name' in Employee._fields)
        self.assertTrue('salary' in Employee._fields)
        self.assertEqual(Employee._get_collection_name(),
                         self.Person._get_collection_name())

        # Ensure that MRO error is not raised
        class A(Document):
            meta = {'allow_inheritance': True}
        class B(A): pass
        class C(B): pass

    def test_allow_inheritance(self):
        """Ensure that inheritance may be disabled on simple classes and that
        _cls will not be used.
        """

        class Animal(Document):
            name = StringField()
            meta = {'allow_inheritance': False}

        Animal.drop_collection()
        def create_dog_class():
            class Dog(Animal):
                pass
        self.assertRaises(ValueError, create_dog_class)

        # Check that _cls etc aren't present on simple documents
        dog = Animal(name='dog')
        dog.save()
        collection = self.db[Animal._get_collection_name()]
        obj = collection.find_one()
        self.assertFalse('_cls' in obj)

        Animal.drop_collection()

        def create_employee_class():
            class Employee(self.Person):
                meta = {'allow_inheritance': False}
        self.assertRaises(ValueError, create_employee_class)

        # Test the same for embedded documents
        class Comment(EmbeddedDocument):
            content = StringField()
            meta = {'allow_inheritance': False}

        def create_special_comment():
            class SpecialComment(Comment):
                pass
        self.assertRaises(ValueError, create_special_comment)

        comment = Comment(content='test')
        self.assertFalse('_cls' in comment.to_mongo())

    def test_allow_inheritance_abstract_document(self):
        """Ensure that abstract documents can set inheritance rules and that
        _cls will not be used.
        """
        class FinalDocument(Document):
            meta = {'abstract': True,
                    'allow_inheritance': False}

        class Animal(FinalDocument):
            name = StringField()

        Animal.drop_collection()
        def create_dog_class():
            class Dog(Animal):
                pass
        self.assertRaises(ValueError, create_dog_class)

        # Check that _cls etc aren't present on simple documents
        dog = Animal(name='dog')
        dog.save()
        collection = self.db[Animal._get_collection_name()]
        obj = collection.find_one()
        self.assertFalse('_cls' in obj)

        Animal.drop_collection()

    def test_abstract_documents(self):
        """Ensure that a document superclass can be marked as abstract
        thereby not using it as the name for the collection."""

        class Animal(Document):
            name = StringField()
            meta = {'abstract': True}

        class Fish(Animal): pass
        class Guppy(Fish): pass

        class Mammal(Animal):
            meta = {'abstract': True}
        class Human(Mammal): pass

        self.assertFalse('collection' in Animal._meta)
        self.assertFalse('collection' in Mammal._meta)

        self.assertEqual(Animal._get_collection_name(), None)
        self.assertEqual(Mammal._get_collection_name(), None)

        self.assertEqual(Fish._get_collection_name(), 'fish')
        self.assertEqual(Guppy._get_collection_name(), 'fish')
        self.assertEqual(Human._get_collection_name(), 'human')

        def create_bad_abstract():
            class EvilHuman(Human):
                evil = BooleanField(default=True)
                meta = {'abstract': True}
        self.assertRaises(ValueError, create_bad_abstract)

    def test_collection_name(self):
        """Ensure that a collection with a specified name may be used.
        """
        collection = 'personCollTest'
        if collection in self.db.collection_names():
            self.db.drop_collection(collection)

        class Person(Document):
            name = StringField()
            meta = {'collection': collection}

        user = Person(name="Test User")
        user.save()
        self.assertTrue(collection in self.db.collection_names())

        user_obj = self.db[collection].find_one()
        self.assertEqual(user_obj['name'], "Test User")

        user_obj = Person.objects[0]
        self.assertEqual(user_obj.name, "Test User")

        Person.drop_collection()
        self.assertFalse(collection in self.db.collection_names())

    def test_collection_name_and_primary(self):
        """Ensure that a collection with a specified name may be used.
        """

        class Person(Document):
            name = StringField(primary_key=True)
            meta = {'collection': 'app'}

        user = Person(name="Test User")
        user.save()

        user_obj = Person.objects[0]
        self.assertEqual(user_obj.name, "Test User")

        Person.drop_collection()

    def test_inherited_collections(self):
        """Ensure that subclassed documents don't override parents' collections.
        """
        with warnings.catch_warnings(record=True) as w:
            # Cause all warnings to always be triggered.
            warnings.simplefilter("always")

            class Drink(Document):
                name = StringField()

            class AlcoholicDrink(Drink):
                meta = {'collection': 'booze'}

            class Drinker(Document):
                drink = GenericReferenceField()

            # Confirm we triggered a SyntaxWarning
            assert issubclass(w[0].category, SyntaxWarning)

        Drink.drop_collection()
        AlcoholicDrink.drop_collection()
        Drinker.drop_collection()

        red_bull = Drink(name='Red Bull')
        red_bull.save()

        programmer = Drinker(drink=red_bull)
        programmer.save()

        beer = AlcoholicDrink(name='Beer')
        beer.save()
        real_person = Drinker(drink=beer)
        real_person.save()

        self.assertEqual(Drinker.objects[0].drink.name, red_bull.name)
        self.assertEqual(Drinker.objects[1].drink.name, beer.name)

    def test_capped_collection(self):
        """Ensure that capped collections work properly.
        """
        class Log(Document):
            date = DateTimeField(default=datetime.now)
            meta = {
                'max_documents': 10,
                'max_size': 90000,
            }

        Log.drop_collection()

        # Ensure that the collection handles up to its maximum
        for i in range(10):
            Log().save()

        self.assertEqual(len(Log.objects), 10)

        # Check that extra documents don't increase the size
        Log().save()
        self.assertEqual(len(Log.objects), 10)

        options = Log.objects._collection.options()
        self.assertEqual(options['capped'], True)
        self.assertEqual(options['max'], 10)
        self.assertEqual(options['size'], 90000)

        # Check that the document cannot be redefined with different options
        def recreate_log_document():
            class Log(Document):
                date = DateTimeField(default=datetime.now)
                meta = {
                    'max_documents': 11,
                }
            # Create the collection by accessing Document.objects
            Log.objects
        self.assertRaises(InvalidCollectionError, recreate_log_document)

        Log.drop_collection()

    def test_hint(self):

        class BlogPost(Document):
            tags = ListField(StringField())
            meta = {
                'indexes': [
                    'tags',
                ],
            }

        BlogPost.drop_collection()

        for i in xrange(0, 10):
            tags = [("tag %i" % n) for n in xrange(0, i % 2)]
            BlogPost(tags=tags).save()

        self.assertEquals(BlogPost.objects.count(), 10)
        self.assertEquals(BlogPost.objects.hint().count(), 10)
        self.assertEquals(BlogPost.objects.hint([('tags', 1)]).count(), 10)

        self.assertEquals(BlogPost.objects.hint([('ZZ', 1)]).count(), 10)

        def invalid_index():
            list(BlogPost.objects.hint('tags'))
        self.assertRaises(pymongo.errors.OperationFailure, invalid_index)

        def invalid_index_2():
            return list(BlogPost.objects.hint([('tags', 1)]))
        self.assertRaises(pymongo.errors.OperationFailure, invalid_index_2)

    def test_custom_id_field(self):
        """Ensure that documents may be created with custom primary keys.
        """
        class User(Document):
            username = StringField(primary_key=True)
            name = StringField()

            meta = {'allow_inheritance': True}

        User.drop_collection()

        self.assertEqual(User._fields['username'].db_field, '_id')
        self.assertEqual(User._meta['id_field'], 'username')

        def create_invalid_user():
            User(name='test').save() # no primary key field
        self.assertRaises(ValidationError, create_invalid_user)

        def define_invalid_user():
            class EmailUser(User):
                email = StringField(primary_key=True)
        self.assertRaises(ValueError, define_invalid_user)

        class EmailUser(User):
            email = StringField()

        user = User(username='test', name='test user')
        user.save()

        user_obj = User.objects.first()
        self.assertEqual(user_obj.id, 'test')
        self.assertEqual(user_obj.pk, 'test')

        user_son = User.objects._collection.find_one()
        self.assertEqual(user_son['_id'], 'test')
        self.assertTrue('username' not in user_son['_id'])

        User.drop_collection()

        user = User(pk='mongo', name='mongo user')
        user.save()

        user_obj = User.objects.first()
        self.assertEqual(user_obj.id, 'mongo')
        self.assertEqual(user_obj.pk, 'mongo')

        user_son = User.objects._collection.find_one()
        self.assertEqual(user_son['_id'], 'mongo')
        self.assertTrue('username' not in user_son['_id'])

        User.drop_collection()


    def test_document_not_registered(self):

        class Place(Document):
            name = StringField()

            meta = {'allow_inheritance': True}

        class NicePlace(Place):
            pass

        Place.drop_collection()

        Place(name="London").save()
        NicePlace(name="Buckingham Palace").save()

        # Mimic Place and NicePlace definitions being in a different file
        # and the NicePlace model not being imported in at query time.
        from mongoengine.base import _document_registry
        del(_document_registry['Place.NicePlace'])

        def query_without_importing_nice_place():
            print Place.objects.all()
        self.assertRaises(NotRegistered, query_without_importing_nice_place)


    def test_creation(self):
        """Ensure that document may be created using keyword arguments.
        """
        person = self.Person(name="Test User", age=30)
        self.assertEqual(person.name, "Test User")
        self.assertEqual(person.age, 30)

    def test_to_dbref(self):
        """Ensure that you can get a dbref of a document"""
        person = self.Person(name="Test User", age=30)
        self.assertRaises(OperationError, person.to_dbref)
        person.save()

        person.to_dbref()

    def test_reload(self):
        """Ensure that attributes may be reloaded.
        """
        person = self.Person(name="Test User", age=20)
        person.save()

        person_obj = self.Person.objects.first()
        person_obj.name = "Mr Test User"
        person_obj.age = 21
        person_obj.save()

        self.assertEqual(person.name, "Test User")
        self.assertEqual(person.age, 20)

        person.reload()
        self.assertEqual(person.name, "Mr Test User")
        self.assertEqual(person.age, 21)

    def test_reload_referencing(self):
        """Ensures reloading updates weakrefs correctly
        """
        class Embedded(EmbeddedDocument):
            dict_field = DictField()
            list_field = ListField()

        class Doc(Document):
            dict_field = DictField()
            list_field = ListField()
            embedded_field = EmbeddedDocumentField(Embedded)

        Doc.drop_collection()
        doc = Doc()
        doc.dict_field = {'hello': 'world'}
        doc.list_field = ['1', 2, {'hello': 'world'}]

        embedded_1 = Embedded()
        embedded_1.dict_field = {'hello': 'world'}
        embedded_1.list_field = ['1', 2, {'hello': 'world'}]
        doc.embedded_field = embedded_1
        doc.save()

        doc = doc.reload(10)
        doc.list_field.append(1)
        doc.dict_field['woot'] = "woot"
        doc.embedded_field.list_field.append(1)
        doc.embedded_field.dict_field['woot'] = "woot"

        self.assertEquals(doc._get_changed_fields(), [
            'list_field', 'dict_field', 'embedded_field.list_field',
            'embedded_field.dict_field'])
        doc.save()

        doc = doc.reload(10)
        self.assertEquals(doc._get_changed_fields(), [])
        self.assertEquals(len(doc.list_field), 4)
        self.assertEquals(len(doc.dict_field), 2)
        self.assertEquals(len(doc.embedded_field.list_field), 4)
        self.assertEquals(len(doc.embedded_field.dict_field), 2)

    def test_dictionary_access(self):
        """Ensure that dictionary-style field access works properly.
        """
        person = self.Person(name='Test User', age=30)
        self.assertEquals(person['name'], 'Test User')

        self.assertRaises(KeyError, person.__getitem__, 'salary')
        self.assertRaises(KeyError, person.__setitem__, 'salary', 50)

        person['name'] = 'Another User'
        self.assertEquals(person['name'], 'Another User')

        # Length = length(assigned fields + id)
        self.assertEquals(len(person), 3)

        self.assertTrue('age' in person)
        person.age = None
        self.assertFalse('age' in person)
        self.assertFalse('nationality' in person)

    def test_embedded_document(self):
        """Ensure that embedded documents are set up correctly.
        """
        class Comment(EmbeddedDocument):
            content = StringField()

        self.assertTrue('content' in Comment._fields)
        self.assertFalse('id' in Comment._fields)
        self.assertFalse('collection' in Comment._meta)

    def test_embedded_document_validation(self):
        """Ensure that embedded documents may be validated.
        """
        class Comment(EmbeddedDocument):
            date = DateTimeField()
            content = StringField(required=True)

        comment = Comment()
        self.assertRaises(ValidationError, comment.validate)

        comment.content = 'test'
        comment.validate()

        comment.date = 4
        self.assertRaises(ValidationError, comment.validate)

        comment.date = datetime.now()
        comment.validate()

    def test_save(self):
        """Ensure that a document may be saved in the database.
        """
        # Create person object and save it to the database
        person = self.Person(name='Test User', age=30)
        person.save()
        # Ensure that the object is in the database
        collection = self.db[self.Person._get_collection_name()]
        person_obj = collection.find_one({'name': 'Test User'})
        self.assertEqual(person_obj['name'], 'Test User')
        self.assertEqual(person_obj['age'], 30)
        self.assertEqual(person_obj['_id'], person.id)
        # Test skipping validation on save
        class Recipient(Document):
            email = EmailField(required=True)

        recipient = Recipient(email='root@localhost')
        self.assertRaises(ValidationError, recipient.save)
        try:
            recipient.save(validate=False)
        except ValidationError:
            self.fail()

    def test_save_to_a_value_that_equates_to_false(self):

        class Thing(EmbeddedDocument):
            count = IntField()

        class User(Document):
            thing = EmbeddedDocumentField(Thing)

        User.drop_collection()

        user = User(thing=Thing(count=1))
        user.save()
        user.reload()

        user.thing.count = 0
        user.save()

        user.reload()
        self.assertEquals(user.thing.count, 0)

    def test_save_max_recursion_not_hit(self):

        class Person(Document):
            name = StringField()
            parent = ReferenceField('self')
            friend = ReferenceField('self')

        Person.drop_collection()

        p1 = Person(name="Wilson Snr")
        p1.parent = None
        p1.save()

        p2 = Person(name="Wilson Jr")
        p2.parent = p1
        p2.save()

        p1.friend = p2
        p1.save()

        # Confirm can save and it resets the changed fields without hitting
        # max recursion error
        p0 = Person.objects.first()
        p0.name = 'wpjunior'
        p0.save()

    def test_save_cascades(self):

        class Person(Document):
            name = StringField()
            parent = ReferenceField('self')

        Person.drop_collection()

        p1 = Person(name="Wilson Snr")
        p1.parent = None
        p1.save()

        p2 = Person(name="Wilson Jr")
        p2.parent = p1
        p2.save()

        p = Person.objects(name="Wilson Jr").get()
        p.parent.name = "Daddy Wilson"
        p.save()

        p1.reload()
        self.assertEquals(p1.name, p.parent.name)

    def test_save_cascade_kwargs(self):

        class Person(Document):
            name = StringField()
            parent = ReferenceField('self')

        Person.drop_collection()

        p1 = Person(name="Wilson Snr")
        p1.parent = None
        p1.save()

        p2 = Person(name="Wilson Jr")
        p2.parent = p1
        p2.save(force_insert=True, cascade_kwargs={"force_insert": False})

        p = Person.objects(name="Wilson Jr").get()
        p.parent.name = "Daddy Wilson"
        p.save()

        p1.reload()
        self.assertEquals(p1.name, p.parent.name)

    def test_save_cascade_meta(self):

        class Person(Document):
            name = StringField()
            parent = ReferenceField('self')

            meta = {'cascade': False}

        Person.drop_collection()

        p1 = Person(name="Wilson Snr")
        p1.parent = None
        p1.save()

        p2 = Person(name="Wilson Jr")
        p2.parent = p1
        p2.save()

        p = Person.objects(name="Wilson Jr").get()
        p.parent.name = "Daddy Wilson"
        p.save()

        p1.reload()
        self.assertNotEquals(p1.name, p.parent.name)

        p.save(cascade=True)
        p1.reload()
        self.assertEquals(p1.name, p.parent.name)

    def test_save_cascades_generically(self):

        class Person(Document):
            name = StringField()
            parent = GenericReferenceField()

        Person.drop_collection()

        p1 = Person(name="Wilson Snr")
        p1.save()

        p2 = Person(name="Wilson Jr")
        p2.parent = p1
        p2.save()

        p = Person.objects(name="Wilson Jr").get()
        p.parent.name = "Daddy Wilson"
        p.save()

        p1.reload()
        self.assertEquals(p1.name, p.parent.name)

    def test_update(self):
        """Ensure that an existing document is updated instead of be overwritten.
        """
        # Create person object and save it to the database
        person = self.Person(name='Test User', age=30)
        person.save()

        # Create same person object, with same id, without age
        same_person = self.Person(name='Test')
        same_person.id = person.id
        same_person.save()

        # Confirm only one object
        self.assertEquals(self.Person.objects.count(), 1)

        # reload
        person.reload()
        same_person.reload()

        # Confirm the same
        self.assertEqual(person, same_person)
        self.assertEqual(person.name, same_person.name)
        self.assertEqual(person.age, same_person.age)

        # Confirm the saved values
        self.assertEqual(person.name, 'Test')
        self.assertEqual(person.age, 30)

        # Test only / exclude only updates included fields
        person = self.Person.objects.only('name').get()
        person.name = 'User'
        person.save()

        person.reload()
        self.assertEqual(person.name, 'User')
        self.assertEqual(person.age, 30)

        # test exclude only updates set fields
        person = self.Person.objects.exclude('name').get()
        person.age = 21
        person.save()

        person.reload()
        self.assertEqual(person.name, 'User')
        self.assertEqual(person.age, 21)

        # Test only / exclude can set non excluded / included fields
        person = self.Person.objects.only('name').get()
        person.name = 'Test'
        person.age = 30
        person.save()

        person.reload()
        self.assertEqual(person.name, 'Test')
        self.assertEqual(person.age, 30)

        # test exclude only updates set fields
        person = self.Person.objects.exclude('name').get()
        person.name = 'User'
        person.age = 21
        person.save()

        person.reload()
        self.assertEqual(person.name, 'User')
        self.assertEqual(person.age, 21)

        # Confirm does remove unrequired fields
        person = self.Person.objects.exclude('name').get()
        person.age = None
        person.save()

        person.reload()
        self.assertEqual(person.name, 'User')
        self.assertEqual(person.age, None)

        person = self.Person.objects.get()
        person.name = None
        person.age = None
        person.save()

        person.reload()
        self.assertEqual(person.name, None)
        self.assertEqual(person.age, None)

    def test_document_update(self):

        def update_not_saved_raises():
            person = self.Person(name='dcrosta')
            person.update(set__name='Dan Crosta')

        self.assertRaises(OperationError, update_not_saved_raises)

        author = self.Person(name='dcrosta')
        author.save()

        author.update(set__name='Dan Crosta')
        author.reload()

        p1 = self.Person.objects.first()
        self.assertEquals(p1.name, author.name)

        def update_no_value_raises():
            person = self.Person.objects.first()
            person.update()

        self.assertRaises(OperationError, update_no_value_raises)

        def update_no_op_raises():
            person = self.Person.objects.first()
            person.update(name="Dan")

        self.assertRaises(InvalidQueryError, update_no_op_raises)

    def test_embedded_update(self):
        """
        Test update on `EmbeddedDocumentField` fields
        """

        class Page(EmbeddedDocument):
            log_message = StringField(verbose_name="Log message",
                                      required=True)

        class Site(Document):
            page = EmbeddedDocumentField(Page)


        Site.drop_collection()
        site = Site(page=Page(log_message="Warning: Dummy message"))
        site.save()

        # Update
        site = Site.objects.first()
        site.page.log_message = "Error: Dummy message"
        site.save()

        site = Site.objects.first()
        self.assertEqual(site.page.log_message, "Error: Dummy message")

    def test_embedded_update_db_field(self):
        """
        Test update on `EmbeddedDocumentField` fields when db_field is other
        than default.
        """

        class Page(EmbeddedDocument):
            log_message = StringField(verbose_name="Log message",
                                      db_field="page_log_message",
                                      required=True)

        class Site(Document):
            page = EmbeddedDocumentField(Page)


        Site.drop_collection()

        site = Site(page=Page(log_message="Warning: Dummy message"))
        site.save()

        # Update
        site = Site.objects.first()
        site.page.log_message = "Error: Dummy message"
        site.save()

        site = Site.objects.first()
        self.assertEqual(site.page.log_message, "Error: Dummy message")

    def test_delta(self):

        class Doc(Document):
            string_field = StringField()
            int_field = IntField()
            dict_field = DictField()
            list_field = ListField()

        Doc.drop_collection()
        doc = Doc()
        doc.save()

        doc = Doc.objects.first()
        self.assertEquals(doc._get_changed_fields(), [])
        self.assertEquals(doc._delta(), ({}, {}))

        doc.string_field = 'hello'
        self.assertEquals(doc._get_changed_fields(), ['string_field'])
        self.assertEquals(doc._delta(), ({'string_field': 'hello'}, {}))

        doc._changed_fields = []
        doc.int_field = 1
        self.assertEquals(doc._get_changed_fields(), ['int_field'])
        self.assertEquals(doc._delta(), ({'int_field': 1}, {}))

        doc._changed_fields = []
        dict_value = {'hello': 'world', 'ping': 'pong'}
        doc.dict_field = dict_value
        self.assertEquals(doc._get_changed_fields(), ['dict_field'])
        self.assertEquals(doc._delta(), ({'dict_field': dict_value}, {}))

        doc._changed_fields = []
        list_value = ['1', 2, {'hello': 'world'}]
        doc.list_field = list_value
        self.assertEquals(doc._get_changed_fields(), ['list_field'])
        self.assertEquals(doc._delta(), ({'list_field': list_value}, {}))

        # Test unsetting
        doc._changed_fields = []
        doc.dict_field = {}
        self.assertEquals(doc._get_changed_fields(), ['dict_field'])
        self.assertEquals(doc._delta(), ({}, {'dict_field': 1}))

        doc._changed_fields = []
        doc.list_field = []
        self.assertEquals(doc._get_changed_fields(), ['list_field'])
        self.assertEquals(doc._delta(), ({}, {'list_field': 1}))

    def test_delta_recursive(self):

        class Embedded(EmbeddedDocument):
            string_field = StringField()
            int_field = IntField()
            dict_field = DictField()
            list_field = ListField()

        class Doc(Document):
            string_field = StringField()
            int_field = IntField()
            dict_field = DictField()
            list_field = ListField()
            embedded_field = EmbeddedDocumentField(Embedded)

        Doc.drop_collection()
        doc = Doc()
        doc.save()

        doc = Doc.objects.first()
        self.assertEquals(doc._get_changed_fields(), [])
        self.assertEquals(doc._delta(), ({}, {}))

        embedded_1 = Embedded()
        embedded_1.string_field = 'hello'
        embedded_1.int_field = 1
        embedded_1.dict_field = {'hello': 'world'}
        embedded_1.list_field = ['1', 2, {'hello': 'world'}]
        doc.embedded_field = embedded_1

        self.assertEquals(doc._get_changed_fields(), ['embedded_field'])

        embedded_delta = {
            'string_field': 'hello',
            'int_field': 1,
            'dict_field': {'hello': 'world'},
            'list_field': ['1', 2, {'hello': 'world'}]
        }
        self.assertEquals(doc.embedded_field._delta(), (embedded_delta, {}))
        embedded_delta.update({
            '_cls': 'Embedded',
        })
        self.assertEquals(doc._delta(), ({'embedded_field': embedded_delta}, {}))

        doc.save()
        doc = doc.reload(10)

        doc.embedded_field.dict_field = {}
        self.assertEquals(doc._get_changed_fields(), ['embedded_field.dict_field'])
        self.assertEquals(doc.embedded_field._delta(), ({}, {'dict_field': 1}))
        self.assertEquals(doc._delta(), ({}, {'embedded_field.dict_field': 1}))
        doc.save()
        doc = doc.reload(10)
        self.assertEquals(doc.embedded_field.dict_field, {})

        doc.embedded_field.list_field = []
        self.assertEquals(doc._get_changed_fields(), ['embedded_field.list_field'])
        self.assertEquals(doc.embedded_field._delta(), ({}, {'list_field': 1}))
        self.assertEquals(doc._delta(), ({}, {'embedded_field.list_field': 1}))
        doc.save()
        doc = doc.reload(10)
        self.assertEquals(doc.embedded_field.list_field, [])

        embedded_2 = Embedded()
        embedded_2.string_field = 'hello'
        embedded_2.int_field = 1
        embedded_2.dict_field = {'hello': 'world'}
        embedded_2.list_field = ['1', 2, {'hello': 'world'}]

        doc.embedded_field.list_field = ['1', 2, embedded_2]
        self.assertEquals(doc._get_changed_fields(), ['embedded_field.list_field'])
        self.assertEquals(doc.embedded_field._delta(), ({
            'list_field': ['1', 2, {
                '_cls': 'Embedded',
                'string_field': 'hello',
                'dict_field': {'hello': 'world'},
                'int_field': 1,
                'list_field': ['1', 2, {'hello': 'world'}],
            }]
        }, {}))

        self.assertEquals(doc._delta(), ({
            'embedded_field.list_field': ['1', 2, {
                '_cls': 'Embedded',
                 'string_field': 'hello',
                 'dict_field': {'hello': 'world'},
                 'int_field': 1,
                 'list_field': ['1', 2, {'hello': 'world'}],
            }]
        }, {}))
        doc.save()
        doc = doc.reload(10)

        self.assertEquals(doc.embedded_field.list_field[0], '1')
        self.assertEquals(doc.embedded_field.list_field[1], 2)
        # TODO COLIN: Fix?
        return
        for k in doc.embedded_field.list_field[2]._fields:
            self.assertEquals(doc.embedded_field.list_field[2][k], embedded_2[k])

        doc.embedded_field.list_field[2].string_field = 'world'
        self.assertEquals(doc._get_changed_fields(), ['embedded_field.list_field.2.string_field'])
        self.assertEquals(doc.embedded_field._delta(), ({'list_field.2.string_field': 'world'}, {}))
        self.assertEquals(doc._delta(), ({'embedded_field.list_field.2.string_field': 'world'}, {}))
        doc.save()
        doc = doc.reload(10)
        self.assertEquals(doc.embedded_field.list_field[2].string_field, 'world')

        # Test multiple assignments
        doc.embedded_field.list_field[2].string_field = 'hello world'
        doc.embedded_field.list_field[2] = doc.embedded_field.list_field[2]
        self.assertEquals(doc._get_changed_fields(), ['embedded_field.list_field'])
        self.assertEquals(doc.embedded_field._delta(), ({
            'list_field': ['1', 2, {
            '_cls': 'Embedded',
            'string_field': 'hello world',
            'int_field': 1,
            'list_field': ['1', 2, {'hello': 'world'}],
            'dict_field': {'hello': 'world'}}]}, {}))
        self.assertEquals(doc._delta(), ({
            'embedded_field.list_field': ['1', 2, {
                '_cls': 'Embedded',
                'string_field': 'hello world',
                'int_field': 1,
                'list_field': ['1', 2, {'hello': 'world'}],
                'dict_field': {'hello': 'world'}}
            ]}, {}))
        doc.save()
        doc = doc.reload(10)
        self.assertEquals(doc.embedded_field.list_field[2].string_field, 'hello world')

        # Test list native methods
        doc.embedded_field.list_field[2].list_field.pop(0)
        self.assertEquals(doc._delta(), ({'embedded_field.list_field.2.list_field': [2, {'hello': 'world'}]}, {}))
        doc.save()
        doc = doc.reload(10)

        doc.embedded_field.list_field[2].list_field.append(1)
        self.assertEquals(doc._delta(), ({'embedded_field.list_field.2.list_field': [2, {'hello': 'world'}, 1]}, {}))
        doc.save()
        doc = doc.reload(10)
        self.assertEquals(doc.embedded_field.list_field[2].list_field, [2, {'hello': 'world'}, 1])

        doc.embedded_field.list_field[2].list_field.sort()
        doc.save()
        doc = doc.reload(10)
        self.assertEquals(doc.embedded_field.list_field[2].list_field, [1, 2, {'hello': 'world'}])

        del(doc.embedded_field.list_field[2].list_field[2]['hello'])
        self.assertEquals(doc._delta(), ({'embedded_field.list_field.2.list_field': [1, 2, {}]}, {}))
        doc.save()
        doc = doc.reload(10)

        del(doc.embedded_field.list_field[2].list_field)
        self.assertEquals(doc._delta(), ({}, {'embedded_field.list_field.2.list_field': 1}))

        doc.save()
        doc = doc.reload(10)

        doc.dict_field['Embedded'] = embedded_1
        doc.save()
        doc = doc.reload(10)

        doc.dict_field['Embedded'].string_field = 'Hello World'
        self.assertEquals(doc._get_changed_fields(), ['dict_field.Embedded.string_field'])
        self.assertEquals(doc._delta(), ({'dict_field.Embedded.string_field': 'Hello World'}, {}))


    def test_delta_db_field(self):

        class Doc(Document):
            string_field = StringField(db_field='db_string_field')
            int_field = IntField(db_field='db_int_field')
            dict_field = DictField(db_field='db_dict_field')
            list_field = ListField(db_field='db_list_field')

        Doc.drop_collection()
        doc = Doc()
        doc.save()

        doc = Doc.objects.first()
        self.assertEquals(doc._get_changed_fields(), [])
        self.assertEquals(doc._delta(), ({}, {}))

        doc.string_field = 'hello'
        self.assertEquals(doc._get_changed_fields(), ['db_string_field'])
        self.assertEquals(doc._delta(), ({'db_string_field': 'hello'}, {}))

        doc._changed_fields = []
        doc.int_field = 1
        self.assertEquals(doc._get_changed_fields(), ['db_int_field'])
        self.assertEquals(doc._delta(), ({'db_int_field': 1}, {}))

        doc._changed_fields = []
        dict_value = {'hello': 'world', 'ping': 'pong'}
        doc.dict_field = dict_value
        self.assertEquals(doc._get_changed_fields(), ['db_dict_field'])
        self.assertEquals(doc._delta(), ({'db_dict_field': dict_value}, {}))

        doc._changed_fields = []
        list_value = ['1', 2, {'hello': 'world'}]
        doc.list_field = list_value
        self.assertEquals(doc._get_changed_fields(), ['db_list_field'])
        self.assertEquals(doc._delta(), ({'db_list_field': list_value}, {}))

        # Test unsetting
        doc._changed_fields = []
        doc.dict_field = {}
        self.assertEquals(doc._get_changed_fields(), ['db_dict_field'])
        self.assertEquals(doc._delta(), ({}, {'db_dict_field': 1}))

        doc._changed_fields = []
        doc.list_field = []
        self.assertEquals(doc._get_changed_fields(), ['db_list_field'])
        self.assertEquals(doc._delta(), ({}, {'db_list_field': 1}))

        # Test it saves that data
        doc = Doc()
        doc.save()

        doc.string_field = 'hello'
        doc.int_field = 1
        doc.dict_field = {'hello': 'world'}
        doc.list_field = ['1', 2, {'hello': 'world'}]
        doc.save()
        doc = doc.reload(10)

        self.assertEquals(doc.string_field, 'hello')
        self.assertEquals(doc.int_field, 1)
        self.assertEquals(doc.dict_field, {'hello': 'world'})
        self.assertEquals(doc.list_field, ['1', 2, {'hello': 'world'}])

    def test_delta_recursive_db_field(self):

        class Embedded(EmbeddedDocument):
            string_field = StringField(db_field='db_string_field')
            int_field = IntField(db_field='db_int_field')
            dict_field = DictField(db_field='db_dict_field')
            list_field = ListField(db_field='db_list_field')

        class Doc(Document):
            string_field = StringField(db_field='db_string_field')
            int_field = IntField(db_field='db_int_field')
            dict_field = DictField(db_field='db_dict_field')
            list_field = ListField(db_field='db_list_field')
            embedded_field = EmbeddedDocumentField(Embedded, db_field='db_embedded_field')

        Doc.drop_collection()
        doc = Doc()
        doc.save()

        doc = Doc.objects.first()
        self.assertEquals(doc._get_changed_fields(), [])
        self.assertEquals(doc._delta(), ({}, {}))

        embedded_1 = Embedded()
        embedded_1.string_field = 'hello'
        embedded_1.int_field = 1
        embedded_1.dict_field = {'hello': 'world'}
        embedded_1.list_field = ['1', 2, {'hello': 'world'}]
        doc.embedded_field = embedded_1

        self.assertEquals(doc._get_changed_fields(), ['db_embedded_field'])

        embedded_delta = {
            'db_string_field': 'hello',
            'db_int_field': 1,
            'db_dict_field': {'hello': 'world'},
            'db_list_field': ['1', 2, {'hello': 'world'}]
        }
        self.assertEquals(doc.embedded_field._delta(), (embedded_delta, {}))
        embedded_delta.update({
            '_cls': 'Embedded',
        })
        self.assertEquals(doc._delta(), ({'db_embedded_field': embedded_delta}, {}))

        doc.save()
        doc = doc.reload(10)

        doc.embedded_field.dict_field = {}
        self.assertEquals(doc._get_changed_fields(), ['db_embedded_field.db_dict_field'])
        self.assertEquals(doc.embedded_field._delta(), ({}, {'db_dict_field': 1}))
        self.assertEquals(doc._delta(), ({}, {'db_embedded_field.db_dict_field': 1}))
        doc.save()
        doc = doc.reload(10)
        self.assertEquals(doc.embedded_field.dict_field, {})

        doc.embedded_field.list_field = []
        self.assertEquals(doc._get_changed_fields(), ['db_embedded_field.db_list_field'])
        self.assertEquals(doc.embedded_field._delta(), ({}, {'db_list_field': 1}))
        self.assertEquals(doc._delta(), ({}, {'db_embedded_field.db_list_field': 1}))
        doc.save()
        doc = doc.reload(10)
        self.assertEquals(doc.embedded_field.list_field, [])

        embedded_2 = Embedded()
        embedded_2.string_field = 'hello'
        embedded_2.int_field = 1
        embedded_2.dict_field = {'hello': 'world'}
        embedded_2.list_field = ['1', 2, {'hello': 'world'}]

        doc.embedded_field.list_field = ['1', 2, embedded_2]
        self.assertEquals(doc._get_changed_fields(), ['db_embedded_field.db_list_field'])
        self.assertEquals(doc.embedded_field._delta(), ({
            'db_list_field': ['1', 2, {
                '_cls': 'Embedded',
                'db_string_field': 'hello',
                'db_dict_field': {'hello': 'world'},
                'db_int_field': 1,
                'db_list_field': ['1', 2, {'hello': 'world'}],
            }]
        }, {}))

        self.assertEquals(doc._delta(), ({
            'db_embedded_field.db_list_field': ['1', 2, {
                '_cls': 'Embedded',
                 'db_string_field': 'hello',
                 'db_dict_field': {'hello': 'world'},
                 'db_int_field': 1,
                 'db_list_field': ['1', 2, {'hello': 'world'}],
            }]
        }, {}))
        doc.save()
        doc = doc.reload(10)

        self.assertEquals(doc.embedded_field.list_field[0], '1')
        self.assertEquals(doc.embedded_field.list_field[1], 2)

        doc.embedded_field.list_field[2]['string_field'] = 'world'
        # TODO: COLIN: Fix?
        #self.assertEquals(doc._get_changed_fields(), ['db_embedded_field.db_list_field.2.db_string_field'])
        #self.assertEquals(doc.embedded_field._delta(), ({'db_list_field.2.db_string_field': 'world'}, {}))
        #self.assertEquals(doc._delta(), ({'db_embedded_field.db_list_field.2.db_string_field': 'world'}, {}))
        #doc.save()
        #doc = doc.reload(10)
        #self.assertEquals(doc.embedded_field.list_field[2].string_field, 'world')
        return

        # Test multiple assignments
        doc.embedded_field.list_field[2].string_field = 'hello world'
        doc.embedded_field.list_field[2] = doc.embedded_field.list_field[2]
        self.assertEquals(doc._get_changed_fields(), ['db_embedded_field.db_list_field'])
        self.assertEquals(doc.embedded_field._delta(), ({
            'db_list_field': ['1', 2, {
            '_cls': 'Embedded',
            'db_string_field': 'hello world',
            'db_int_field': 1,
            'db_list_field': ['1', 2, {'hello': 'world'}],
            'db_dict_field': {'hello': 'world'}}]}, {}))
        self.assertEquals(doc._delta(), ({
            'db_embedded_field.db_list_field': ['1', 2, {
                '_cls': 'Embedded',
                'db_string_field': 'hello world',
                'db_int_field': 1,
                'db_list_field': ['1', 2, {'hello': 'world'}],
                'db_dict_field': {'hello': 'world'}}
            ]}, {}))
        doc.save()
        doc = doc.reload(10)
        self.assertEquals(doc.embedded_field.list_field[2].string_field, 'hello world')

        # Test list native methods
        doc.embedded_field.list_field[2].list_field.pop(0)
        self.assertEquals(doc._delta(), ({'db_embedded_field.db_list_field.2.db_list_field': [2, {'hello': 'world'}]}, {}))
        doc.save()
        doc = doc.reload(10)

        doc.embedded_field.list_field[2].list_field.append(1)
        self.assertEquals(doc._delta(), ({'db_embedded_field.db_list_field.2.db_list_field': [2, {'hello': 'world'}, 1]}, {}))
        doc.save()
        doc = doc.reload(10)
        self.assertEquals(doc.embedded_field.list_field[2].list_field, [2, {'hello': 'world'}, 1])

        doc.embedded_field.list_field[2].list_field.sort()
        doc.save()
        doc = doc.reload(10)
        self.assertEquals(doc.embedded_field.list_field[2].list_field, [1, 2, {'hello': 'world'}])

        del(doc.embedded_field.list_field[2].list_field[2]['hello'])
        self.assertEquals(doc._delta(), ({'db_embedded_field.db_list_field.2.db_list_field': [1, 2, {}]}, {}))
        doc.save()
        doc = doc.reload(10)

        del(doc.embedded_field.list_field[2].list_field)
        self.assertEquals(doc._delta(), ({}, {'db_embedded_field.db_list_field.2.db_list_field': 1}))

    def test_save_only_changed_fields(self):
        """Ensure save only sets / unsets changed fields
        """

        class User(self.Person):
            active = BooleanField(default=True)


        User.drop_collection()

        # Create person object and save it to the database
        user = User(name='Test User', age=30, active=True)
        user.save()
        user.reload()

        # Simulated Race condition
        same_person = self.Person.objects.get()
        same_person.active = False

        user.age = 21
        user.save()

        same_person.name = 'User'
        same_person.save()

        person = self.Person.objects.get()
        self.assertEquals(person.name, 'User')
        self.assertEquals(person.age, 21)
        self.assertEquals(person.active, False)

    def test_delete(self):
        """Ensure that document may be deleted using the delete method.
        """
        person = self.Person(name="Test User", age=30)
        person.save()
        self.assertEqual(len(self.Person.objects), 1)
        person.delete()
        self.assertEqual(len(self.Person.objects), 0)

    def test_save_custom_id(self):
        """Ensure that a document may be saved with a custom _id.
        """
        # Create person object and save it to the database
        person = self.Person(name='Test User', age=30,
                             id='497ce96f395f2f052a494fd4')
        person.save()
        # Ensure that the object is in the database with the correct _id
        collection = self.db[self.Person._get_collection_name()]
        person_obj = collection.find_one({'name': 'Test User'})
        self.assertEqual(str(person_obj['_id']), '497ce96f395f2f052a494fd4')

    def test_save_custom_pk(self):
        """Ensure that a document may be saved with a custom _id using pk alias.
        """
        # Create person object and save it to the database
        person = self.Person(name='Test User', age=30,
                             pk='497ce96f395f2f052a494fd4')
        person.save()
        # Ensure that the object is in the database with the correct _id
        collection = self.db[self.Person._get_collection_name()]
        person_obj = collection.find_one({'name': 'Test User'})
        self.assertEqual(str(person_obj['_id']), '497ce96f395f2f052a494fd4')

    def test_save_list(self):
        """Ensure that a list field may be properly saved.
        """
        class Comment(EmbeddedDocument):
            content = StringField()

        class BlogPost(Document):
            content = StringField()
            comments = ListField(EmbeddedDocumentField(Comment))
            tags = ListField(StringField())

        BlogPost.drop_collection()

        post = BlogPost(content='Went for a walk today...')
        post.tags = tags = ['fun', 'leisure']
        comments = [Comment(content='Good for you'), Comment(content='Yay.')]
        post.comments = comments
        post.save()

        collection = self.db[BlogPost._get_collection_name()]
        post_obj = collection.find_one()
        self.assertEqual(post_obj['tags'], tags)
        for comment_obj, comment in zip(post_obj['comments'], comments):
            self.assertEqual(comment_obj['content'], comment['content'])

        BlogPost.drop_collection()

    def test_list_search_by_embedded(self):
        class User(Document):
            username = StringField(required=True)

            meta = {'allow_inheritance': False}

        class Comment(EmbeddedDocument):
            comment = StringField()
            user = ReferenceField(User,
                                  required=True)

            meta = {'allow_inheritance': False}

        class Page(Document):
            comments = ListField(EmbeddedDocumentField(Comment))
            meta = {'allow_inheritance': False,
                    'indexes': [
                        {'fields': ['comments.user']}
                    ]}

        User.drop_collection()
        Page.drop_collection()

        u1 = User(username="wilson")
        u1.save()

        u2 = User(username="rozza")
        u2.save()

        u3 = User(username="hmarr")
        u3.save()

        p1 = Page(comments = [Comment(user=u1, comment="Its very good"),
                              Comment(user=u2, comment="Hello world"),
                              Comment(user=u3, comment="Ping Pong"),
                              Comment(user=u1, comment="I like a beer")])
        p1.save()

        p2 = Page(comments = [Comment(user=u1, comment="Its very good"),
                              Comment(user=u2, comment="Hello world")])
        p2.save()

        p3 = Page(comments = [Comment(user=u3, comment="Its very good")])
        p3.save()

        p4 = Page(comments = [Comment(user=u2, comment="Heavy Metal song")])
        p4.save()

        self.assertEqual([p1, p2], list(Page.objects.filter(comments__user=u1)))
        self.assertEqual([p1, p2, p4], list(Page.objects.filter(comments__user=u2)))
        self.assertEqual([p1, p3], list(Page.objects.filter(comments__user=u3)))

    def test_save_embedded_document(self):
        """Ensure that a document with an embedded document field may be
        saved in the database.
        """
        class EmployeeDetails(EmbeddedDocument):
            position = StringField()

        class Employee(self.Person):
            salary = IntField()
            details = EmbeddedDocumentField(EmployeeDetails)

        # Create employee object and save it to the database
        employee = Employee(name='Test Employee', age=50, salary=20000)
        employee.details = EmployeeDetails(position='Developer')
        employee.save()

        # Ensure that the object is in the database
        collection = self.db[self.Person._get_collection_name()]
        employee_obj = collection.find_one({'name': 'Test Employee'})
        self.assertEqual(employee_obj['name'], 'Test Employee')
        self.assertEqual(employee_obj['age'], 50)
        # Ensure that the 'details' embedded object saved correctly
        self.assertEqual(employee_obj['details']['position'], 'Developer')

    def test_embedded_update_after_save(self):
        """
        Test update of `EmbeddedDocumentField` attached to a newly saved
        document.
        """
        class Page(EmbeddedDocument):
            log_message = StringField(verbose_name="Log message",
                                      required=True)

        class Site(Document):
            page = EmbeddedDocumentField(Page)


        Site.drop_collection()
        site = Site(page=Page(log_message="Warning: Dummy message"))
        site.save()

        # Update
        site.page.log_message = "Error: Dummy message"
        site.save()

        site = Site.objects.first()
        self.assertEqual(site.page.log_message, "Error: Dummy message")

    def test_updating_an_embedded_document(self):
        """Ensure that a document with an embedded document field may be
        saved in the database.
        """
        class EmployeeDetails(EmbeddedDocument):
            position = StringField()

        class Employee(self.Person):
            salary = IntField()
            details = EmbeddedDocumentField(EmployeeDetails)

        # Create employee object and save it to the database
        employee = Employee(name='Test Employee', age=50, salary=20000)
        employee.details = EmployeeDetails(position='Developer')
        employee.save()

        # Test updating an embedded document
        promoted_employee = Employee.objects.get(name='Test Employee')
        promoted_employee.details.position = 'Senior Developer'
        promoted_employee.save()

        promoted_employee.reload()
        self.assertEqual(promoted_employee.name, 'Test Employee')
        self.assertEqual(promoted_employee.age, 50)

        # Ensure that the 'details' embedded object saved correctly
        self.assertEqual(promoted_employee.details.position, 'Senior Developer')

        # Test removal
        promoted_employee.details = None
        promoted_employee.save()

        promoted_employee.reload()
        self.assertEqual(promoted_employee.details, None)

    def test_mixin_inheritance(self):
        class BaseMixIn(object):
            count = IntField()
            data = StringField()

        class DoubleMixIn(BaseMixIn):
            comment = StringField()

        class TestDoc(Document, DoubleMixIn):
            age = IntField()

        TestDoc.drop_collection()
        t = TestDoc(count=12, data="test",
                    comment="great!", age=19)

        t.save()

        t = TestDoc.objects.first()

        self.assertEquals(t.age, 19)
        self.assertEquals(t.comment, "great!")
        self.assertEquals(t.data, "test")
        self.assertEquals(t.count, 12)

    def test_save_reference(self):
        """Ensure that a document reference field may be saved in the database.
        """

        class BlogPost(Document):
            meta = {'collection': 'blogpost_1'}
            content = StringField()
            author = ReferenceField(self.Person)

        BlogPost.drop_collection()

        author = self.Person(name='Test User')
        author.save()

        post = BlogPost(content='Watched some TV today... how exciting.')
        # Should only reference author when saving
        post.author = author
        post.save()

        post_obj = BlogPost.objects.first()

        # Test laziness
        self.assertTrue(isinstance(post_obj._data['author'],
                                   bson.DBRef))
        self.assertTrue(isinstance(post_obj.author, self.Person))
        self.assertEqual(post_obj.author.name, 'Test User')

        # Ensure that the dereferenced object may be changed and saved
        post_obj.author.age = 25
        post_obj.author.save()

        author = list(self.Person.objects(name='Test User'))[-1]
        self.assertEqual(author.age, 25)

        BlogPost.drop_collection()

    def test_cannot_perform_joins_references(self):

        class BlogPost(Document):
            author = ReferenceField(self.Person)
            author2 = GenericReferenceField()

        def test_reference():
            list(BlogPost.objects(author__name="test"))

        self.assertRaises(InvalidQueryError, test_reference)

        def test_generic_reference():
            list(BlogPost.objects(author2__name="test"))

        self.assertRaises(InvalidQueryError, test_generic_reference)

    def test_duplicate_db_fields_raise_invalid_document_error(self):
        """Ensure a InvalidDocumentError is thrown if duplicate fields
        declare the same db_field"""

        def throw_invalid_document_error():
            class Foo(Document):
                name = StringField()
                name2 = StringField(db_field='name')

        self.assertRaises(InvalidDocumentError, throw_invalid_document_error)

    def test_reverse_delete_rule_cascade_and_nullify(self):
        """Ensure that a referenced document is also deleted upon deletion.
        """

        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person, reverse_delete_rule=CASCADE)
            reviewer = ReferenceField(self.Person, reverse_delete_rule=NULLIFY)

        self.Person.drop_collection()
        BlogPost.drop_collection()

        author = self.Person(name='Test User')
        author.save()

        reviewer = self.Person(name='Re Viewer')
        reviewer.save()

        post = BlogPost(content = 'Watched some TV')
        post.author = author
        post.reviewer = reviewer
        post.save()

        reviewer.delete()
        self.assertEqual(len(BlogPost.objects), 1)  # No effect on the BlogPost
        self.assertEqual(BlogPost.objects.get().reviewer, None)

        # Delete the Person, which should lead to deletion of the BlogPost, too
        author.delete()
        self.assertEqual(len(BlogPost.objects), 0)

    def test_reverse_delete_rule_cascade_and_nullify_complex_field(self):
        """Ensure that a referenced document is also deleted upon deletion.
        """

        class BlogPost(Document):
            content = StringField()
            authors = ListField(ReferenceField(self.Person, reverse_delete_rule=CASCADE))
            reviewers = ListField(ReferenceField(self.Person, reverse_delete_rule=NULLIFY))

        self.Person.drop_collection()
        BlogPost.drop_collection()

        author = self.Person(name='Test User')
        author.save()

        reviewer = self.Person(name='Re Viewer')
        reviewer.save()

        post = BlogPost(content= 'Watched some TV')
        post.authors = [author]
        post.reviewers = [reviewer]
        post.save()

        reviewer.delete()
        self.assertEqual(len(BlogPost.objects), 1)  # No effect on the BlogPost
        self.assertEqual(BlogPost.objects.get().reviewers, [])

        # Delete the Person, which should lead to deletion of the BlogPost, too
        author.delete()
        self.assertEqual(len(BlogPost.objects), 0)

    def test_invalid_reverse_delete_rules_raise_errors(self):

        def throw_invalid_document_error():
            class Blog(Document):
                content = StringField()
                authors = MapField(ReferenceField(self.Person, reverse_delete_rule=CASCADE))
                reviewers = DictField(field=ReferenceField(self.Person, reverse_delete_rule=NULLIFY))

        self.assertRaises(InvalidDocumentError, throw_invalid_document_error)

        def throw_invalid_document_error_embedded():
            class Parents(EmbeddedDocument):
                father = ReferenceField('Person', reverse_delete_rule=DENY)
                mother = ReferenceField('Person', reverse_delete_rule=DENY)

        self.assertRaises(InvalidDocumentError, throw_invalid_document_error_embedded)

    def test_reverse_delete_rule_cascade_recurs(self):
        """Ensure that a chain of documents is also deleted upon cascaded
        deletion.
        """

        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person, reverse_delete_rule=CASCADE)

        class Comment(Document):
            text = StringField()
            post = ReferenceField(BlogPost, reverse_delete_rule=CASCADE)

        self.Person.drop_collection()
        BlogPost.drop_collection()
        Comment.drop_collection()

        author = self.Person(name='Test User')
        author.save()

        post = BlogPost(content = 'Watched some TV')
        post.author = author
        post.save()

        comment = Comment(text = 'Kudos.')
        comment.post = post
        comment.save()

        # Delete the Person, which should lead to deletion of the BlogPost, and,
        # recursively to the Comment, too
        author.delete()
        self.assertEqual(len(Comment.objects), 0)

        self.Person.drop_collection()
        BlogPost.drop_collection()
        Comment.drop_collection()

    def test_reverse_delete_rule_deny(self):
        """Ensure that a document cannot be referenced if there are still
        documents referring to it.
        """

        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(self.Person, reverse_delete_rule=DENY)

        self.Person.drop_collection()
        BlogPost.drop_collection()

        author = self.Person(name='Test User')
        author.save()

        post = BlogPost(content = 'Watched some TV')
        post.author = author
        post.save()

        # Delete the Person should be denied
        self.assertRaises(OperationError, author.delete)  # Should raise denied error
        self.assertEqual(len(BlogPost.objects), 1)  # No objects may have been deleted
        self.assertEqual(len(self.Person.objects), 1)

        # Other users, that don't have BlogPosts must be removable, like normal
        author = self.Person(name='Another User')
        author.save()

        self.assertEqual(len(self.Person.objects), 2)
        author.delete()
        self.assertEqual(len(self.Person.objects), 1)

        self.Person.drop_collection()
        BlogPost.drop_collection()

    def subclasses_and_unique_keys_works(self):

        class A(Document):
            pass

        class B(A):
            foo = BooleanField(unique=True)

        A.drop_collection()
        B.drop_collection()

        A().save()
        A().save()
        B(foo=True).save()

        self.assertEquals(A.objects.count(), 2)
        self.assertEquals(B.objects.count(), 1)
        A.drop_collection()
        B.drop_collection()

    def test_document_hash(self):
        """Test document in list, dict, set
        """
        class User(Document):
            pass

        class BlogPost(Document):
            pass

        # Clear old datas
        User.drop_collection()
        BlogPost.drop_collection()

        u1 = User.objects.create()
        u2 = User.objects.create()
        u3 = User.objects.create()
        u4 = User() # New object

        b1 = BlogPost.objects.create()
        b2 = BlogPost.objects.create()

        # in List
        all_user_list = list(User.objects.all())

        self.assertTrue(u1 in all_user_list)
        self.assertTrue(u2 in all_user_list)
        self.assertTrue(u3 in all_user_list)
        self.assertFalse(u4 in all_user_list) # New object
        self.assertFalse(b1 in all_user_list) # Other object
        self.assertFalse(b2 in all_user_list) # Other object

        # in Dict
        all_user_dic = {}
        for u in User.objects.all():
            all_user_dic[u] = "OK"

        self.assertEqual(all_user_dic.get(u1, False), "OK" )
        self.assertEqual(all_user_dic.get(u2, False), "OK" )
        self.assertEqual(all_user_dic.get(u3, False), "OK" )
        self.assertEqual(all_user_dic.get(u4, False), False ) # New object
        self.assertEqual(all_user_dic.get(b1, False), False ) # Other object
        self.assertEqual(all_user_dic.get(b2, False), False ) # Other object

        # in Set
        all_user_set = set(User.objects.all())

        self.assertTrue(u1 in all_user_set )

    def test_picklable(self):

        pickle_doc = PickleTest(number=1, string="One", lists=['1', '2'])
        pickle_doc.embedded = PickleEmbedded()
        pickle_doc.save()

        pickled_doc = pickle.dumps(pickle_doc)
        resurrected = pickle.loads(pickled_doc)

        self.assertEquals(resurrected, pickle_doc)

        resurrected.string = "Two"
        resurrected.save()

        pickle_doc = pickle_doc.reload()
        self.assertEquals(resurrected, pickle_doc)

    def test_throw_invalid_document_error(self):

        # test handles people trying to upsert
        def throw_invalid_document_error():
            class Blog(Document):
                validate = DictField()

        self.assertRaises(InvalidDocumentError, throw_invalid_document_error)

    def test_mutating_documents(self):

        class B(EmbeddedDocument):
            field1 = StringField(default='field1')

        class A(Document):
            b = EmbeddedDocumentField(B, default=lambda: B())

        A.drop_collection()
        a = A()
        a.save()
        a.reload()
        self.assertEquals(a.b.field1, 'field1')

        class C(EmbeddedDocument):
            c_field = StringField(default='cfield')

        class B(EmbeddedDocument):
            field1 = StringField(default='field1')
            field2 = EmbeddedDocumentField(C, default=lambda: C())

        class A(Document):
            b = EmbeddedDocumentField(B, default=lambda: B())

        a = A.objects()[0]
        a.b.field2.c_field = 'new value'
        a.save()

        a.reload()
        self.assertEquals(a.b.field2.c_field, 'new value')

    def test_can_save_false_values(self):
        """Ensures you can save False values on save"""
        class Doc(Document):
            foo = StringField()
            archived = BooleanField(default=False, required=True)

        Doc.drop_collection()
        d = Doc()
        d.save()
        d.archived = False
        d.save()

        self.assertEquals(Doc.objects(archived=False).count(), 1)


    def test_can_save_false_values_dynamic(self):
        """Ensures you can save False values on dynamic docs"""
        class Doc(DynamicDocument):
            foo = StringField()

        Doc.drop_collection()
        d = Doc()
        d.save()
        d.archived = False
        d.save()

        self.assertEquals(Doc.objects(archived=False).count(), 1)

    def test_do_not_save_unchanged_references(self):
        """Ensures cascading saves dont auto update"""
        class Job(Document):
            name = StringField()

        class Person(Document):
            name = StringField()
            age = IntField()
            job = ReferenceField(Job)

        Job.drop_collection()
        Person.drop_collection()

        job = Job(name="Job 1")
        # job should not have any changed fields after the save
        job.save()

        person = Person(name="name", age=10, job=job)

        from pymongo.collection import Collection
        orig_update = Collection.update
        try:
            def fake_update(*args, **kwargs):
                self.fail("Unexpected update for %s" % args[0].name)
                return orig_update(*args, **kwargs)

            Collection.update = fake_update
            person.save()
        finally:
            Collection.update = orig_update

    def test_db_alias_tests(self):
        """ DB Alias tests """
        # mongoenginetest - Is default connection alias from setUp()
        # Register Aliases
        register_db('mongoenginetest2', 'testdb-1')
        register_db('mongoenginetest3', 'testdb-2')
        register_db('mongoenginetest4', 'testdb-3')

        class User(Document):
            name = StringField()
            meta = {"db_alias": "testdb-1"}

        class Book(Document):
            name = StringField()
            meta = {"db_alias": "testdb-2"}

        # Drops
        User.drop_collection()
        Book.drop_collection()

        # Create
        bob = User.objects.create(name="Bob")
        hp = Book.objects.create(name="Harry Potter")

        # Selects
        self.assertEqual(User.objects.first(), bob)
        self.assertEqual(Book.objects.first(), hp)

        # DeRefecence
        class AuthorBooks(Document):
            author = ReferenceField(User)
            book = ReferenceField(Book)
            meta = {"db_alias": "testdb-3"}

        # Drops
        AuthorBooks.drop_collection()

        ab = AuthorBooks.objects.create(author=bob, book=hp)

        # select
        self.assertEqual(AuthorBooks.objects.first(), ab)
        self.assertEqual(AuthorBooks.objects.first().book, hp)
        self.assertEqual(AuthorBooks.objects.first().author, bob)
        self.assertEqual(AuthorBooks.objects.filter(author=bob).first(), ab)
        self.assertEqual(AuthorBooks.objects.filter(book=hp).first(), ab)

        # DB Alias
        self.assertEqual(User._get_db(), get_db("testdb-1"))
        self.assertEqual(Book._get_db(), get_db("testdb-2"))
        self.assertEqual(AuthorBooks._get_db(), get_db("testdb-3"))

        # Collections
        self.assertEqual(User._get_collection(), get_db("testdb-1")[User._get_collection_name()])
        self.assertEqual(Book._get_collection(), get_db("testdb-2")[Book._get_collection_name()])
        self.assertEqual(AuthorBooks._get_collection(), get_db("testdb-3")[AuthorBooks._get_collection_name()])

    def test_db_ref_usage(self):
        """ DB Ref usage in __raw__ queries """

        class User(Document):
            name = StringField()

        class Book(Document):
            name = StringField()
            author = ReferenceField(User)
            extra = DictField()
            meta = {
                'ordering': ['+name']
            }

            def __unicode__(self):
                return self.name

            def __str__(self):
                return self.name

        # Drops
        User.drop_collection()
        Book.drop_collection()

        # Authors
        bob = User.objects.create(name="Bob")
        jon = User.objects.create(name="Jon")

        # Redactors
        karl = User.objects.create(name="Karl")
        susan = User.objects.create(name="Susan")
        peter = User.objects.create(name="Peter")

        # Bob
        Book.objects.create(name="1", author=bob, extra={"a": bob.to_dbref(), "b": [karl.to_dbref(), susan.to_dbref()]})
        Book.objects.create(name="2", author=bob, extra={"a": bob.to_dbref(), "b": karl.to_dbref()} )
        Book.objects.create(name="3", author=bob, extra={"a": bob.to_dbref(), "c": [jon.to_dbref(), peter.to_dbref()]})
        Book.objects.create(name="4", author=bob)

        # Jon
        Book.objects.create(name="5", author=jon)
        Book.objects.create(name="6", author=peter)
        Book.objects.create(name="7", author=jon)
        Book.objects.create(name="8", author=jon)
        Book.objects.create(name="9", author=jon, extra={"a": peter.to_dbref()})

        # Checks
        self.assertEqual(u",".join([str(b) for b in Book.objects.all()] ) , "1,2,3,4,5,6,7,8,9" )
        # bob related books
        self.assertEqual(u",".join([str(b) for b in Book.objects.filter(
                                    Q(extra__a=bob ) |
                                    Q(author=bob) |
                                    Q(extra__b=bob))]) ,
                                    "1,2,3,4")

        # Susan & Karl related books
        self.assertEqual(u",".join([str(b) for b in Book.objects.filter(
                                    Q(extra__a__all=[karl, susan] ) |
                                    Q(author__all=[karl, susan ] ) |
                                    Q(extra__b__all=[karl.to_dbref(), susan.to_dbref()] )
                                    ) ] ) , "1" )

        # $Where
        self.assertEqual(u",".join([str(b) for b in Book.objects.filter(
                                    __raw__={
                                        "$where": """
                                            function(){
                                                return this.name == '1' ||
                                                       this.name == '2';}"""
                                        }
                                    ) ]), "1,2")

if __name__ == '__main__':
    unittest.main()
