import json
import datetime
import sys
import os
import random

from utils.node_rpc_wrapper import NodeRpcWrapper
from utils.telegram_wrapper import TelegramWrapper
from utils.discord_wrapper import DiscordWrapper


def check_and_send_reward_collection_message(telegram, discord, cfg, cached_epoch, new_epoch):
    channel_id = cfg['telegram_channel_id']
    dev_chat_id = cfg['telegram_dev_chat_id']
    discord_webhook_url = cfg['discord_channel_webhook']

    if new_epoch > cached_epoch:
        m = create_reward_collection_message(new_epoch)
        if 'error' in m:
            handle_error(telegram, dev_chat_id, m['error'])
        else:
            r = telegram.bot_send_message_to_chat(channel_id, m['message'])
            print(
                f'Reward collection message sent to Telegram: {r.status_code}')

            if len(discord_webhook_url) > 0:
                r = discord.webhook_send_message_to_channel(
                    discord_webhook_url, m['message'])
                print(
                    f'Reward collection message sent to Discord: {r.status_code}')


def check_and_send_missed_momentums_message(telegram, discord, cfg, cached_pillars, new_pillars, cached_momentum_status_data, cache_file):
    channel_id = cfg['telegram_channel_id']
    dev_chat_id = cfg['telegram_dev_chat_id']
    discord_webhook_url = cfg['discord_channel_webhook']

    new_momentum_status_data = {}
    inactive_pillars = []
    for owner_address in new_pillars:
        pillar_name = new_pillars[owner_address]['name']
        missed_momentums_in_a_row = 0
        is_producing = True

        if owner_address in cached_pillars and owner_address in cached_momentum_status_data:

            # Get the pillar's information from cache
            missed_momentums_in_a_row = cached_momentum_status_data[owner_address]['missedMomentums']
            is_producing = cached_momentum_status_data[owner_address]['isProducing']                

            previous_produced_momentums = cached_pillars[
                owner_address]['currentStats']['producedMomentums']
            current_produced_momentums = new_pillars[owner_address]['currentStats']['producedMomentums']
            previous_expected_momentums = cached_pillars[
                owner_address]['currentStats']['expectedMomentums']
            current_expected_momentums = new_pillars[owner_address]['currentStats']['expectedMomentums']

            # Handle epoch change
            if current_produced_momentums == 0 and previous_produced_momentums > 0:
                if not is_producing:
                    inactive_pillars.append(owner_address)

            # Handle normal case        
            else:
                # Check if pillar has produced new momentums
                if current_produced_momentums == previous_produced_momentums:

                    # Check if the amount of expected momentums has changed
                    if current_expected_momentums != previous_expected_momentums:
                        missed_momentums_in_a_row = missed_momentums_in_a_row + 1
                        if missed_momentums_in_a_row >= 5:
                            inactive_pillars.append(owner_address)
                            is_producing = False
                    
                    # If expected mometum amount has not changed and pillar was previously inactive, add to inactive pillars list
                    elif not is_producing:
                        inactive_pillars.append(owner_address)
                
                # If pillar has produced new momentums set missed momentum count to zero
                else:
                    missed_momentums_in_a_row = 0
                    is_producing = True
            
        # Add pillar's new momentum status data
        new_momentum_status_data[owner_address] = {'name': pillar_name, 'missedMomentums': missed_momentums_in_a_row, 'isProducing': is_producing}  
    
    l = []
    for address in inactive_pillars:
        if address in new_momentum_status_data:
            l.append(new_momentum_status_data[address]['name'])
    if len(l) > 0:
        print('Inactive pillars: ' + str(l))

    # Save new data
    write_to_file_as_json({'data': new_momentum_status_data, 'timestamp': str(datetime.datetime.now())}, cache_file)

    for address in inactive_pillars:
        # Verify the inactive pillar was previously producing momentums until sending message
        if address in cached_momentum_status_data and cached_momentum_status_data[address]['isProducing']:
            m = create_pillar_inactive_message(cached_momentum_status_data[address]['name'])
            if 'error' in m:
                handle_error(telegram, dev_chat_id, m['error'])
            else:
                r = telegram.bot_send_message_to_chat(channel_id, m['message'])
            print(
                f'Pillar inactive message sent to Telegram: {r.status_code}')
            if len(discord_webhook_url) > 0:
                r = discord.webhook_send_message_to_channel(discord_webhook_url, m['message'])
                print(
                    f'Pillar inactive message sent to Discord: {r.status_code}')

    for address in cached_momentum_status_data:
        # Check if a previously inactive pillar is not in the inactive pillars list anymore
        if not cached_momentum_status_data[address]['isProducing'] and address not in inactive_pillars:
            m = create_pillar_active_message(cached_momentum_status_data[address]['name'])
            if 'error' in m:
                handle_error(telegram, dev_chat_id, m['error'])
            else:
                r = telegram.bot_send_message_to_chat(channel_id, m['message'])
            print(
                f'Pillar active again message sent to Telegram: {r.status_code}')
            if len(discord_webhook_url) > 0:
                r = discord.webhook_send_message_to_channel(discord_webhook_url, m['message'])
                print(
                    f'Pillar active again message sent to Discord: {r.status_code}')


def check_and_send_pillar_events(telegram, discord, cfg, cached_pillars, new_pillars):
    channel_id = cfg['telegram_channel_id']
    dev_chat_id = cfg['telegram_dev_chat_id']
    discord_webhook_url = cfg['discord_channel_webhook']

    # Check for dismantled Pillars. Assume Pillar is dismantled if the owner address is no longer present in the new data.
    for owner_address in cached_pillars:
       if owner_address not in new_pillars and len(new_pillars) < len(cached_pillars):
           m = create_dismantled_pillar_message(
               cached_pillars[owner_address])
           if 'error' in m:
               handle_error(telegram, dev_chat_id, m['error'])
           else:
               name = cached_pillars[owner_address]['name']
               r = telegram.bot_send_message_to_chat(channel_id, m['message'])
               print(
                   f'Pillar dismantled message sent to Telegram ({name}): {r.status_code}')
    
               if len(discord_webhook_url) > 0:
                   r = discord.webhook_send_message_to_channel(
                       discord_webhook_url, m['message'])
                   print(
                       f'Pillar dismantled message sent to Discord ({name}): {r.status_code}')

    # Check for new Pillars. Assume Pillar is new if the owner address was not present in the cached data.
    for owner_address in new_pillars:
        if owner_address not in cached_pillars and len(new_pillars) > len(cached_pillars):
            m = create_new_pillar_message(
                new_pillars[owner_address])
            if 'error' in m:
                handle_error(telegram, dev_chat_id, m['error'])
            else:
                name = new_pillars[owner_address]['name']

                r = telegram.bot_send_message_to_chat(channel_id, m['message'])
                print(
                    f'Pillar created message sent to Telegram ({name}): {r.status_code}')

                if len(discord_webhook_url) > 0:
                    r = discord.webhook_send_message_to_channel(
                        discord_webhook_url, m['message'])
                    print(
                        f'Pillar created message sent to Discord ({name}): {r.status_code}')

    # Check for Pillar name changes
    for owner_address in new_pillars:
        if owner_address in cached_pillars:

            # Get current and cached name
            current_name = new_pillars[owner_address]['name']
            cached_name = cached_pillars[owner_address]['name']

            if current_name != cached_name:
                m = create_pillar_name_changed_message(
                    cached_name, current_name)
                if 'error' in m:
                    handle_error(telegram, dev_chat_id, m['error'])
                else:
                    r = telegram.bot_send_message_to_chat(
                        channel_id, m['message'])
                    print(
                        f'Pillar name changed message sent to Telegram ({cached_name} -> {current_name}): {r.status_code}')

                    if len(discord_webhook_url) > 0:
                        r = discord.webhook_send_message_to_channel(
                            discord_webhook_url, m['message'])
                        print(
                            f'Pillar name changed message sent to Discord ({cached_name} -> {current_name}): {r.status_code}')

    # Check for changes in reward sharing
    for owner_address in new_pillars:
        if owner_address in cached_pillars:
            old_momentum_percentage = cached_pillars[
                owner_address]['giveMomentumRewardPercentage']
            new_momentum_percentage = new_pillars[owner_address]['giveMomentumRewardPercentage']
            old_delegate_percentage = cached_pillars[
                owner_address]['giveDelegateRewardPercentage']
            new_delegate_percentage = new_pillars[owner_address]['giveDelegateRewardPercentage']
            name = new_pillars[owner_address]['name']
            owner_address = new_pillars[owner_address]['ownerAddress']
            changed_shares_data = {}

            if old_momentum_percentage != new_momentum_percentage:
                changed_shares_data['name'] = name
                changed_shares_data['ownerAddress'] = owner_address
                changed_shares_data['momentumRewards'] = {
                    'oldMomentumPercentage': old_momentum_percentage, 'newMomentumPercentage': new_momentum_percentage}

            if old_delegate_percentage != new_delegate_percentage:
                changed_shares_data['name'] = name
                changed_shares_data['ownerAddress'] = owner_address
                changed_shares_data['delegateRewards'] = {
                    'oldDelegatePercentage': old_delegate_percentage, 'newDelegatePercentage': new_delegate_percentage}

            if changed_shares_data != {}:
                if 'momentumRewards' not in changed_shares_data:
                    changed_shares_data['momentumRewards'] = {
                        'oldMomentumPercentage': old_momentum_percentage}
                if 'delegateRewards' not in changed_shares_data:
                    changed_shares_data['delegateRewards'] = {
                        'oldDelegatePercentage': old_delegate_percentage}

                m = create_reward_share_changed_message(changed_shares_data)
                if 'error' in m:
                    handle_error(telegram, dev_chat_id, m['error'])
                else:
                    name = new_pillars[owner_address]['name']
                    r = telegram.bot_send_message_to_chat(
                        channel_id, m['message'])
                    print(
                        f'Reward share changed message sent to Telegram ({name}): {r.status_code}')

                    if len(discord_webhook_url) > 0:
                        r = discord.webhook_send_message_to_channel(
                            discord_webhook_url, m['message'])
                        print(
                            f'Reward share changed message sent to Discord ({name}): {r.status_code}')


def create_dismantled_pillar_message(pillar_data):
    try:
        m = pillar_data['name'] + ' has been dismantled.'
        return {'message': m}
    except KeyError:
        return {'error': 'KeyError: create_dismantled_pillar_message'}


def create_new_pillar_message(pillar_data):
    try:
        m = 'New pillar spawned!\n'
        m = m + 'Say hello to ' + pillar_data['name'] + '\n'
        m = m + 'Momentum rewards sharing: ' + \
            str(pillar_data['giveMomentumRewardPercentage']) + '%\n'
        m = m + 'Delegate rewards sharing: ' + \
            str(pillar_data['giveDelegateRewardPercentage']) + '%\n'
        return {'message': m}
    except KeyError:
        return {'error': 'KeyError: create_new_pillar_message'}


def create_pillar_name_changed_message(cached_name, current_name):
    try:
        m = 'Pillar name changed!\n'
        m = m + cached_name + ' \U000027A1 ' + current_name
        return {'message': m}
    except KeyError:
        return {'error': 'KeyError: create_pillar_name_changed_message'}


def create_reward_share_changed_message(changed_shares_data):
    try:
        m = 'Pillar: ' + changed_shares_data['name'] + '\n'

        old_momentum_percentage = changed_shares_data['momentumRewards']['oldMomentumPercentage']
        if ('newMomentumPercentage' in changed_shares_data['momentumRewards']):
            new_momentum_percentage = changed_shares_data['momentumRewards']['newMomentumPercentage']
            m = m + 'Momentum rewards sharing: ' + str(old_momentum_percentage) + \
                '% \U000027A1 ' + str(new_momentum_percentage) + '%\n'
        else:
            m = m + 'Momentum rewards sharing: ' + \
                str(old_momentum_percentage) + '%\n'

        old_delegate_percentage = changed_shares_data['delegateRewards']['oldDelegatePercentage']
        if ('newDelegatePercentage' in changed_shares_data['delegateRewards']):
            new_delegate_percentage = changed_shares_data['delegateRewards']['newDelegatePercentage']
            m = m + 'Delegate rewards sharing: ' + \
                str(old_delegate_percentage) + '% \U000027A1 ' + \
                str(new_delegate_percentage) + '%'
        else:
            m = m + 'Delegate rewards sharing: ' + \
                str(old_delegate_percentage) + '%'

        return {'message': m}

    except KeyError:
        return {'error': 'KeyError: create_reward_share_changed_message'}


def create_pinned_stats_message(pillars, momentum_height):
    try:
        # Only show top 70 Pillars because of Telegram's message character limit (4096 characters)
        if len(pillars) > 70:
            m = 'Pillar reward sharing rates (top 70)\n'
        else:
            m = 'Pillar reward sharing rates\n'
        m = m + 'Last updated: ' + \
            str(datetime.datetime.now(datetime.timezone.utc).strftime(
                '%Y-%m-%d %H:%M:%S')) + ' (UTC)\n'
        m = m + 'Momentum height: ' + str(momentum_height) + '\n'
        m = m + 'M = momentum reward sharing %\n'
        m = m + 'D = delegate reward sharing %\n'
        m = m + 'W = pillar weight (ZNN) \n'
        m = m + 'P/E = produced/expected momentums\n\n'

        for owner_address in pillars:
            if pillars[owner_address]['rank'] < 70:
                weight = int(
                    round(pillars[owner_address]['weight'] / 100000000))
                m = m + str(pillars[owner_address]['rank'] + 1) + ' - ' + str(pillars[owner_address]['name']) + ' -> M: ' + str(pillars[owner_address]['giveMomentumRewardPercentage']) + '% D: ' + str(pillars[owner_address]['giveDelegateRewardPercentage']
                                                                                                                                                                                                        ) + '% W: ' + str(weight) + ' P/E: ' + str(pillars[owner_address]['currentStats']['producedMomentums']) + '/' + str(pillars[owner_address]['currentStats']['expectedMomentums']) + '\n'
        return {'message': m}

    except KeyError:
        return {'error': 'KeyError: create_pinned_stats_message'}


def create_reward_collection_message(reward_epoch):
    try:
        emoji = get_emoji(reward_epoch)
        m = 'Rewards for epoch ' + \
            str(reward_epoch) + ' can now be collected! ' + emoji
        return {'message': m}
    except KeyError:
        return {'error': 'KeyError: create_reward_collection_message'}

def create_pillar_inactive_message(pillar_name):
    try:
        m = pillar_name + ' has stopped producing momentums.'
        return {'message': m}
    except KeyError:
        return {'error': 'KeyError: create_pillar_inactive_message'}

def create_pillar_active_message(pillar_name):
    try:
        m = pillar_name + ' is producing momentums again! \U0001F680'
        return {'message': m}
    except KeyError:
        return {'error': 'KeyError: create_pillar_active_message '}


def get_emoji(epoch):
    emojis = ['\U0001f300', '\U0001F680', '\U0001F47D', '\U0001F60E',
              '\U0001F525', '\U0001F389', '\U0001F31F', '\U0001F440']
    return random.choice(emojis)


def read_file(file_path):
    f = open(file_path)
    content = json.load(f)
    f.close()
    return content


def write_to_file_as_json(data, file_name):
    with open(file_name, 'w') as outfile:
        json.dump(data, outfile, indent=4)


def handle_error(telegram, dev_chat_id, message):
    print(message)

    # Send the developer a message if a developer chat ID is configured
    if len(dev_chat_id) != 0:
        telegram.bot_send_message_to_chat(chat_id=dev_chat_id, message=message)

    # Exit script on error
    sys.exit()


def main():

    # Get current file path
    path = os.path.dirname(os.path.abspath(__file__))

    # Read config
    cfg = read_file(f'{path}/config/config.json')

    # Data store directory
    DATA_STORE_DIR = f'{path}/data_store'

    # Pillar cache file
    PILLAR_CACHE_FILE = f'{DATA_STORE_DIR}/pillar_data.json'

    # Epoch cache file
    EPOCH_CACHE_FILE = f'{DATA_STORE_DIR}/epoch_data.json'

    # Momentum status cache file
    MOMENTUM_STATUS_CACHE_FILE = f'{DATA_STORE_DIR}/momentum_status_data.json'

    # Node status file
    NODE_STATUS_FILE = f'{DATA_STORE_DIR}/node_status_data.json'

    # Check and create data store directory
    if not os.path.exists(DATA_STORE_DIR):
        os.makedirs(DATA_STORE_DIR, exist_ok=True)

    # Check and create pillar cache file
    if not os.path.exists(PILLAR_CACHE_FILE):
        open(PILLAR_CACHE_FILE, 'w+').close()

    # Check and create epoch cache file
    if not os.path.exists(EPOCH_CACHE_FILE):
        open(EPOCH_CACHE_FILE, 'w+').close()

    # Check and create momentum status cache file
    if not os.path.exists(MOMENTUM_STATUS_CACHE_FILE):
        open(MOMENTUM_STATUS_CACHE_FILE, 'w+').close()

    # Check and create node status file
    if not os.path.exists(NODE_STATUS_FILE):
        write_to_file_as_json({'height': 0, 'error': False}, NODE_STATUS_FILE)

    # Create wrappers
    node = NodeRpcWrapper(node_url=cfg['node_url_http'])
    telegram = TelegramWrapper(
        bot_api_key=cfg['telegram_bot_api_key'])
    discord = DiscordWrapper()

    # Get latest momentum
    latest_momentum = node.get_latest_momentum()
    if 'error' in latest_momentum:
        handle_error(
            telegram, cfg['telegram_dev_chat_id'], latest_momentum['error'])

    node_status = read_file(NODE_STATUS_FILE)

    # Check node status
    if latest_momentum['height'] > node_status['height'] and not node_status['error']:
        write_to_file_as_json({'height': latest_momentum['height'], 'error': False}, NODE_STATUS_FILE)
    else:
        write_to_file_as_json({'height': latest_momentum['height'], 'error': True}, NODE_STATUS_FILE)
        handle_error(
            telegram, cfg['telegram_dev_chat_id'], 'Node is stuck. Running prevented.')

    # Get latest Pillar data
    new_pillar_data = node.get_all_pillars()
    if 'error' in new_pillar_data:
        handle_error(
            telegram, cfg['telegram_dev_chat_id'], new_pillar_data['error'])

    # Get reward epoch
    new_epoch_data = node.get_reward_epoch(cfg['reference_reward_address'])
    if 'error' in new_epoch_data:
        handle_error(
            telegram, cfg['telegram_dev_chat_id'], new_epoch_data['error'])

    # Get cached Pillar data from file
    if os.stat(PILLAR_CACHE_FILE).st_size != 0:
        cached_pillar_data = read_file(PILLAR_CACHE_FILE)
    else:
        cached_pillar_data = None

    # Get cached epoch data from file
    if os.stat(EPOCH_CACHE_FILE).st_size != 0:
        cached_epoch_data = read_file(EPOCH_CACHE_FILE)
    else:
        cached_epoch_data = None

    # Get cached momentum status data from file
    if os.stat(MOMENTUM_STATUS_CACHE_FILE).st_size != 0:
        cached_momentum_status_data = read_file(MOMENTUM_STATUS_CACHE_FILE)
    else:
        cached_momentum_status_data = {'data': {}}

    # Cache current Pillar data to file
    write_to_file_as_json(new_pillar_data, PILLAR_CACHE_FILE)

    # Cache current epoch data to file
    write_to_file_as_json(new_epoch_data, EPOCH_CACHE_FILE)

    # Create and update the pinned stats message
    pinned_stats_message = create_pinned_stats_message(
        new_pillar_data['pillars'], latest_momentum['height'])
    if 'error' in pinned_stats_message:
        handle_error(telegram, cfg['telegram_dev_chat_id'],
                     pinned_stats_message['error'])
    else:
        r = telegram.bot_edit_message(
            chat_id=cfg['telegram_channel_id'], message_id=cfg['telegram_pinned_message_id'], message=pinned_stats_message['message'])
        print(f'Pinned message updated on Telegram: {r.status_code}')

    # Check for new Pillar events if cached data exists
    if cached_pillar_data is not None:
        check_and_send_pillar_events(
            telegram, discord, cfg, cached_pillar_data['pillars'], new_pillar_data['pillars'])

    # Check if new rewards are available
    if cached_epoch_data is not None:
        check_and_send_reward_collection_message(
            telegram, discord, cfg, cached_epoch_data['epoch'], new_epoch_data['epoch'])

    # Check for missed momentums
    # TODO: Fix so that momentum status cache is stored on first run as well
    if cached_pillar_data is not None:
        check_and_send_missed_momentums_message(
                telegram, discord, cfg, cached_pillar_data['pillars'], new_pillar_data['pillars'], cached_momentum_status_data['data'], MOMENTUM_STATUS_CACHE_FILE)


if __name__ == '__main__':
    print(f'{str(datetime.datetime.now())}: Starting')
    main()
    print(f'{str(datetime.datetime.now())}: Completed')
