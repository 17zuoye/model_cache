# -*- coding: utf-8 -*-

class LoadData(object):

    @classmethod
    def load_from(cls, original_model):
        """
        """
        from etl_utils import process_notifier
        process_notifier(original_model) # ensure original_model is normal

        extract_model = cls
        print; print "LOAD %s INTO %s" % (original_model.__module__, extract_model.__module__)
        extract_model.init_datadict()

        if extract_model.count() / float(original_model.count()) < cls.original_model_percentage:
            print "[load ids cache] ..."
            ids_cache = {str(zy1.item_id) : True for zy1 in process_notifier(extract_model.datadict.datadict)}

            items = []
            for e1 in process_notifier(original_model):
                #import pdb; pdb.set_trace()
                if cls.original_model_read_id_lambda(e1) in ids_cache: continue
                if cls.original_model_filter_lambda(e1): continue

                items.append(extract_model(e1))

                if len(items) > 10000:
                    extract_model.build_indexes(items)
                    extract_model.datadict.sync()
                    items = []
            del ids_cache
            extract_model.build_indexes(items)
            extract_model.datadict.sync()
