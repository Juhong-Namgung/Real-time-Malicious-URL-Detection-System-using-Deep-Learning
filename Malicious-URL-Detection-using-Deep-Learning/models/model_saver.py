import json
import os
from pathlib import Path
import shutil
import tensorflow as tf

class Saver:
    def __init__(selfs):
        pass

    '''
    Three method to save the trained model    
    
    1) Use SavedModelBuilder
    
    2) Use freeze session
    
    3) Save json, h5 format
    
    '''

    ''' 1) Use SavedModelBuilder '''
    def saved_model_builder(self, session, device, model_name):

        device_path = '../../output/saved_models/' + device
        model_path = device_path + "/" + model_name

        try:
            if not os.path.exists(device_path):
                os.makedirs(device_path)
            # 기존 파일 삭제
            if os.path.exists(model_path):
                shutil.rmtree(model_path)


        except OSError as e:
            print(e)
            print('Error')

        builder = tf.saved_model.builder.SavedModelBuilder(model_path)
        builder.add_meta_graph_and_variables(session, [tf.saved_model.tag_constants.SERVING])
        builder.save()


    ''' 2) Use freeze session '''
    def freeze_session(self, session, keep_var_names=None, output_names="main_output", clear_devices=True):
        """
        Freezes the state of a session into a pruned computation graph.

        Creates a new computation graph where variable nodes are replaced by
        constants taking their current value in the session. The new graph will be
        pruned so subgraphs that are not necessary to compute the requested
        outputs are removed.
        @param session The TensorFlow session to be frozen.
        @param keep_var_names A list of variable names that should not be frozen,
                              or None to freeze all the variables in the graph.
        @param output_names Names of the relevant graph outputs.
        @param clear_devices Remove the device directives from the graph for better portability.
        @return The frozen graph definition.
        """
        graph = session.graph
        with graph.as_default():
            freeze_var_names = list(set(v.op.name for v in tf.global_variables()).difference(keep_var_names or []))
            output_names = output_names or []
            output_names += [v.op.name for v in tf.global_variables()]
            input_graph_def = graph.as_graph_def()
            if clear_devices:
                for node in input_graph_def.node:
                    node.device = ""
            frozen_graph = tf.graph_util.convert_variables_to_constants(
                session, input_graph_def, output_names, freeze_var_names)
            return frozen_graph

    def save_model_use_freezing(self, session, device, model_name, input_tensor, output_tensor):
        signature = tf.saved_model.signature_def_utils.build_signature_def(
            inputs = {'input': tf.saved_model.utils.build_tensor_info(input_tensor)},
            outputs = {'output': tf.saved_model.utils.build_tensor_info(output_tensor)},
        )
        b = tf.saved_model.builder.SavedModelBuilder("../../output/saved_models/" + device + "/" + model_name + "/frezzing/")
        b.add_meta_graph_and_variables(session,
                                       [tf.saved_model.tag_constants.SERVING],
                                       signature_def_map={tf.saved_model.signature_constants.DEFAULT_SERVING_SIGNATURE_DEF_KEY: signature})
        b.save()

    ''' 3) Save json, h5 format '''
    # General save model to disk function
    def save_model_general(self, model, fileModelJSON, fileWeights):
        if Path(fileModelJSON).is_file():
            os.remove(fileModelJSON)
        json_string = model.to_json()

        with open(fileModelJSON, 'w') as f:
            json.dump(json_string, f)

        if Path(fileWeights).is_file():
            os.remove(fileWeights)

        model.save_weights(fileWeights)


