import aerospike
import logging
import sys
from aerospike import exception as ex
from aerospike import predicates as p


def connect_aerospike(config):
    try:
        return aerospike.client(config).connect()
    except ex.AerospikeError as e:
        logging.error("Connection error, host = {0}".format(config['hosts']))
        logging.error("Error: {0} [{1}]".format(e.msg, e.code))
        sys.exit(1)


def create_index(client, namespace, set, bin_name):
    try:
        client.index_string_create(namespace, set, bin_name, bin_name + '_idx')
    except ex.AerospikeError as e:
        logging.error("Create index '{0}' error".format(bin_name))
        logging.error("Error: {0} [{1}]".format(e.msg, e.code))


def add_customer(client, namespace, set, customer_id, phone_number, lifetime_value):
    key = (namespace, set, customer_id)
    try:
        client.put(key, {'phone': phone_number, 'ltv': lifetime_value})
    except ex.AerospikeError as e:
        logging.error("Put record error, idx = {0}".format(customer_id))
        logging.error("Error: {0} [{1}]".format(e.msg, e.code))


def get_ltv_by_customer_id(client, namespace, set, customer_id):
    key = (namespace, set, customer_id)
    try:
        (key, meta, record) = client.get(key)
        return record['ltv']
    except ex.AerospikeError as e:
        logging.error('Get record error: requested non-existent customer {0}'.format(customer_id))
        logging.error("Error: {0} [{1}]".format(e.msg, e.code))


def get_ltv_by_phone(client, namespace, set, phone_number):
    try:
        results = client.query(namespace, set).select('phone', 'ltv').where(p.equals('phone', phone_number)).results()
        if len(results) > 0:
            return results[0][2]['ltv']
        else:
            logging.error('Requested phone number \'{0}\' is not found'.format(phone_number))
    except ex.AerospikeError as e:
        logging.error("Error: {0} [{1}]".format(e.msg, e.code))


if __name__ == '__main__':
    config = {
        'hosts': [('127.0.0.1', 3000)]
    }

    namespace = 'test'
    set = 'phones'

    # connect to the aerospike server
    client = connect_aerospike(config)

    # create second index
    create_index(client, namespace, set, 'phone')

    # add some data
    for i in range(1, 11):
        add_customer(client, namespace, set, i, 'phone_' + str(i), 'ltv_' + str(i))

    # check get operations
    for i in range(1, 11):
        assert ('ltv_' + str(i) == get_ltv_by_customer_id(client, namespace, set, i)), "No LTV by ID " + str(i)
        assert ('ltv_' + str(i) == get_ltv_by_phone(client, namespace, set, 'phone_' + str(i))), "No LTV by phone " + str(i)

    # close connection
    client.close()
