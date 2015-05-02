import pickle
import io
import base64


class CyclicWalkException(Exception):
    pass

# ############################## walk w/ pickle ##############################


def walk(prewalk_fn,
         postwalk_fn,
         obj,
         protocol=pickle.HIGHEST_PROTOCOL):
    """
    walks an arbitrary* python object using pickle.Pickler with a prewalk
    and postwalk function

    * maybe not arbitrary - but probably anything that can be pickled (:
    """
    seen_ids = set()

    def perform_walk(obj, ignore_first_obj):
        """
        ignore_first_obj:
        whether or not to ignore walking the first object encountered
        this is set to ignore walking the first object, to allow recursing
        down objects
        """
        ignore = [ignore_first_obj]

        def persistent_id(obj):
            if ignore[0]:
                # don't walk this element
                ignore[0] = False
                return None
            else:
                if id(obj) in seen_ids:
                    raise CyclicWalkException(
                        "Cannot walk recursive structures")
                else:
                    seen_ids.add(id(obj))

                prewalked = prewalk_fn(obj)
                inner_walked = perform_walk(obj=prewalked,
                                            ignore_first_obj=True)
                postwalked = postwalk_fn(inner_walked)
                # TODO does this really need to be converted to a string
                # seems like it does for python2?
                # base64 encoding to avoid unsafe string errors
                return base64.urlsafe_b64encode(
                    pickle.dumps(postwalked, protocol=protocol))

        def persistent_load(persid):
            return pickle.loads(base64.urlsafe_b64decode(persid))

        src = io.BytesIO()
        pickler = pickle.Pickler(src)
        pickler.persistent_id = persistent_id
        pickler.dump(obj)
        datastream = src.getvalue()
        dst = io.BytesIO(datastream)
        unpickler = pickle.Unpickler(dst)
        unpickler.persistent_load = persistent_load
        return unpickler.load()

    return perform_walk(obj=obj, ignore_first_obj=False)


# ############################ collection walking ############################


def _identity(e):
    return e


def collection_walk(prewalk_fn, postwalk_fn, obj):
    """
    like walk, but more efficient while only working on (predefined)
    collections
    """
    seen_ids = set()

    def perform_walk(obj):
        if id(obj) in seen_ids:
            raise CyclicWalkException(
                "Cannot walk recursive structures")
        else:
            seen_ids.add(id(obj))

        prewalked = prewalk_fn(obj)

        # TODO add more collections
        # eg. namedtuple, ordereddict, numpy array
        # TODO maybe use prewalked.__class__ to construct new instance of same
        # collection
        if isinstance(prewalked, list):
            inner_walked = [perform_walk(item) for item in prewalked]
        elif isinstance(prewalked, dict):
            inner_walked = {perform_walk(key): perform_walk(value)
                            for key, value in prewalked.items()}
        elif isinstance(prewalked, tuple):
            inner_walked = tuple([perform_walk(item) for item in prewalked])
        elif isinstance(prewalked, set):
            inner_walked = {perform_walk(item) for item in prewalked}
        else:
            inner_walked = prewalked

        return postwalk_fn(inner_walked)

    return perform_walk(obj)


def collection_prewalk(prewalk_fn, obj):
    return collection_walk(prewalk_fn, _identity, obj)


def collection_postwalk(postwalk_fn, obj):
    return collection_walk(_identity, postwalk_fn, obj)
