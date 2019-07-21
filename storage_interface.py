import psycopg2
import traceback
import json
import re
from bigchaindb_driver import BigchainDB
from bigchaindb_driver.crypto import generate_keypair


class StorageInterface:
    @staticmethod
    def _execute_sql(query):
        """
        execute write in relational database
        :param query: sql statement
        :return: number of affected rows
        """
        affected_rows = 0
        with open("config/database_configs.json") as file:
            config = json.load(file)
            user_db = config.get("user", None)
            name_db = config.get("name_db", None)
            host_db = config.get("host", None)
            port_db = config.get("port_db", None)
            password_db = config.get("password", None)
        try:
            connection = psycopg2.connect(user=user_db,
                                          password=password_db,
                                          host=host_db,
                                          port=port_db,
                                          database=name_db)
            cursor = connection.cursor()
            cursor.execute(query)
            connection.commit()
            affected_rows = cursor.rowcount
        except (Exception, psycopg2.Error):
            traceback.print_exc()
        finally:
            if connection:
                cursor.close()
                connection.close()
            return affected_rows

    @staticmethod
    def _execute_query_sql(query):
        """
        execute select in relational database
        :param query: sql statement
        :return: list of data
        """
        all_data = []
        with open("config/database_configs.json") as file:
            config = json.load(file)
            user_db = config.get("user", None)
            name_db = config.get("name_db", None)
            host_db = config.get("host", None)
            port_db = config.get("port_db", None)
            password_db = config.get("password", None)
        try:
            connection = psycopg2.connect(user=user_db,
                                          password=password_db,
                                          host=host_db,
                                          port=port_db,
                                          database=name_db)
            cursor = connection.cursor()
            cursor.execute(query)
            data = cursor.fetchall()
            for d in data:
                all_data.append(d)
        except (Exception, psycopg2.Error):
            traceback.print_exc()
        finally:
            if connection:
                cursor.close()
                connection.close()
            return all_data

    @staticmethod
    def _execute_blockchain(query, entity, type_of_req):
        """
        execute write in blockchain
        :param query: sql query
        :param entity: name of entity
        :param type_of_req: update or insert
        :return: number of affected rows
        """
        if type_of_req == "INSERT":
            re_data = re.findall("\(.*?\)", query)
            keys = re_data[0]
            for char in ["(", ")", " "]:
                keys = keys.replace(char, "")
            keys = keys.split(",")
            keys.insert(0, "schema")
            values = re_data[1]
            for char in ["(", ")", "'", "\"", " "]:
                values = values.replace(char, "")
            values = values.split(",")
            values.insert(0, entity)
            data_transaction = dict(zip(keys, values))
            with open("config/bc_configs.json") as file:
                config = json.load(file)
                host_bc = config.get("host", None)
                port_bc = config.get("port", None)
            try:
                bdb = BigchainDB("http://" + str(host_bc) + ":" + str(port_bc))
                keypair = generate_keypair()
                tx = bdb.transactions.prepare(
                    operation='CREATE',
                    signers=keypair.public_key,
                    asset={'data': data_transaction})
                signed_tx = bdb.transactions.fulfill(tx, private_keys=keypair.private_key)
                transaction_sent = bdb.transactions.send_sync(signed_tx)
                StorageInterface._store_index(transaction_sent, entity)
            except:
                traceback.print_exc()
                return 0
            return 1
        else:
            re_update = re.search("(?<=SET ).*(?=WHERE )", query)
            data_update = re_update.group().replace(" ", "").split(",")
            re_conditional = re.search("(?<=WHERE).*", query)
            data_conditional = re_conditional.group().replace(" ", "").split("AND")
            keys = []
            values = []
            for i in data_conditional:
                keys.append(i.split("=")[0])
                values.append(i.split("=")[1])
            for i in data_update:
                keys.append(i.split("=")[0])
                values.append(i.split("=")[1])
            data_transaction = dict(zip(keys, values))
            with open("config/bc_configs.json") as file:
                config = json.load(file)
                host_bc = config.get("host", None)
                port_bc = config.get("port", None)
            try:
                bdb = BigchainDB("http://" + str(host_bc) + ":" + str(port_bc))
                keypair = generate_keypair()
                tx = bdb.transactions.prepare(
                    operation='CREATE',
                    signers=keypair.public_key,
                    asset={'data': data_transaction})
                signed_tx = bdb.transactions.fulfill(tx, private_keys=keypair.private_key)
                transaction_sent = bdb.transactions.send_sync(signed_tx)
                StorageInterface._store_index(transaction_sent, entity)
            except:
                traceback.print_exc()
                return 0
            return 1

    @staticmethod
    def _execute_query_blockchain(query, entity):
        """
        execute selects in blockchain
        :param query: sql query
        :param entity: name of entity
        :return: list of data
        """
        index_ids = StorageInterface._get_asset_id_for_query(query, entity)
        return_data = []
        with open("config/bc_configs.json") as file:
            config = json.load(file)
            host_bc = config.get("host", None)
            port_bc = config.get("port", None)
        bdb = BigchainDB("http://" + str(host_bc) + ":" + str(port_bc))
        for id in index_ids:
            tx = bdb.transactions.get(asset_id=id)
            if tx != []:
                data_list = list(tx[0]["asset"]["data"].values())
                return_data.append(data_list)
        return return_data

    @staticmethod
    def _get_asset_id_for_query(query, entity):
        """
        get all the data ids that are in an entity in the blockchain
        :param query: sql
        :param entity: name of entity
        :return: list of data id
        """
        list_assets_ids = []
        with open("files/index.json") as file:
            json_string = file.read()
            if json_string == "":
                return []
            index = json.loads(json_string)
            entities = index["entities"]
            for e in entities:
                if e["name"] == entity:
                    list_assets_ids = e["ids"]
        return list_assets_ids

    @staticmethod
    def _store_index(transaction_sent, entity_to_save):
        """
        stores the newly recorded transaction id in the blockchain in the index file
        :param transaction_sent: the transaction_id
        :param entity_to_save: the entity of the transaction
        """
        id = transaction_sent.get("id")
        path = "files/index.json"
        with open(path, 'r+') as file:
            text = file.read()
            if text == "":
                new_index = {"entities": [{"name": entity_to_save, "ids": [id]}]}
                json.dump(new_index, file)
            else:
                index = json.loads(text)
                entity_exists = False
                entities = index["entities"]
                for l in entities:
                    if l["name"] == entity_to_save:
                        entity_exists = True
                        l["ids"].append(id)
                        break
                if not entity_exists:
                    index["entities"].append({"name":entity_to_save, "ids":[id]})

                with open(path, 'r+') as file:
                    json.dump(index, file)

    @staticmethod
    def execute(query):
        """
        executes the received query
        :param query: SQL Statement
        :return: the response of the query
        """
        with open("config/schema.json") as file:
            config = json.load(file)
            db_list = config.get("database", [])
            bc_list = config.get("blockchain", [])

        if re.search('insert', query, re.IGNORECASE):
            search_entity = re.search("(?<=INTO ).*", query)
            entity = search_entity.group().split(" ")[0]
            if entity in db_list:
                return StorageInterface._execute_sql(query)
            elif entity in bc_list:
                return StorageInterface._execute_blockchain(query, entity, "INSERT")
            else:
                print("ERROR:", entity,"IS NOT IN SCHEMA")

        elif re.search('select', query, re.IGNORECASE):
            search_entity = re.search("(?<=FROM ).*", query)
            entity = search_entity.group().split(" ")[0]
            if entity in db_list:
                return StorageInterface._execute_query_sql(query)
            elif entity in bc_list:
                return StorageInterface._execute_query_blockchain(query, entity)
            else:
                print("ERROR:", entity,"IS NOT IN SCHEMA")

        elif re.search('delete', query, re.IGNORECASE):
            search_entity = re.search("(?<=FROM ).*", query)
            entity = search_entity.group().split(" ")[0]
            if entity in db_list:
                return StorageInterface._execute_sql(query)
            elif entity in bc_list:
                print("NOT SUPPORTED")
            else:
                print("ERROR:", entity, "IS NOT IN SCHEMA")

        elif re.search('update', query, re.IGNORECASE):
            search_entity = re.search("(?<=UPDATE ).*", query)
            entity = search_entity.group().split(" ")[0]
            if entity in db_list:
                return StorageInterface._execute_sql(query)
            elif entity in bc_list:
                return StorageInterface._execute_blockchain(query, entity, "UPDATE")
            else:
                print("ERROR:", entity, "IS NOT IN SCHEMA")
        else:
            raise Exception()
