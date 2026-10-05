"""Local overlay tests: delay, throttle, cancellation, and shared sender."""
import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch
from plugin.relay.channels.proactive import ProactiveChannel, OUTBOUND_TTL_SECONDS
from plugin.relay import join_wake

class QueueWakeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.channel = ProactiveChannel()
        self.addAsyncCleanup(self.cleanup)
    async def cleanup(self):
        self.channel.cancel_outbound()
        await asyncio.sleep(0)
    async def test_five_minute_delay_fifteen_minute_throttle_first_line_and_count(self):
        ch = self.channel
        with patch('plugin.relay.channels.proactive.time.time', return_value=1000):
            await ch.push({'text':'first\nsecond'})
            await ch.push({'text':'another'})
        with patch('plugin.relay.channels.proactive.wake_queued_phone', new_callable=AsyncMock, return_value=True) as wake:
            for now, expected in [(1299,False),(1300,True),(2199,False),(2200,True)]:
                with patch('plugin.relay.channels.proactive.time.time', return_value=now):
                    self.assertEqual(await ch._check_queue_wake(),expected)
            self.assertEqual(wake.await_count,2)
            self.assertEqual(wake.await_args.args[0]['text'],'first\nsecond')
            self.assertEqual(wake.await_args.args[1],2)
    async def test_subscribe_flush_cancels_wake(self):
        ch = self.channel
        await ch.push({'text':'queued'})
        ws=type('Ws',(),{'closed':False,'send_str':AsyncMock()})()
        task=ch._wake_task
        await ch._handle_subscribe(ws)
        await asyncio.sleep(0)
        self.assertTrue(task.cancelled())
        self.assertEqual(ch.buffered_outbound_count(),0)
        sent=[json.loads(call.args[0]) for call in ws.send_str.await_args_list]
        self.assertEqual(sent[1]['type'],'phone.message')
        self.assertEqual(sent[1]['payload']['text'],'queued')
        self.assertEqual(sent[-1]['type'],'proactive.backlog.complete')
        self.assertFalse(await ch._check_queue_wake())
    async def test_stale_queue_and_cancel_do_not_wake(self):
        ch=self.channel
        await ch.push({'text':'stale'})
        ch._outbound[0]['sent_at']=1000
        with patch('plugin.relay.channels.proactive.time.time',return_value=OUTBOUND_TTL_SECONDS+2):
            self.assertFalse(await ch._check_queue_wake())
        ch.cancel_outbound()
        self.assertFalse(await ch._check_queue_wake())
    async def test_sender_failure_retains_queue_and_throttles(self):
        ch=self.channel
        await ch.push({'text':'waiting'})
        ch._outbound[0]['sent_at']=1000000
        with patch('plugin.relay.channels.proactive.wake_queued_phone',side_effect=OSError('offline')) as wake:
            with patch('plugin.relay.channels.proactive.time.time',return_value=1300):
                self.assertFalse(await ch._check_queue_wake())
            with patch('plugin.relay.channels.proactive.time.time',return_value=1301):
                self.assertFalse(await ch._check_queue_wake())
            self.assertEqual(wake.call_count,1)
        self.assertEqual(ch.buffered_outbound_count(),1)
    async def test_timer_runs_without_additional_push(self):
        ch=self.channel
        ch._outbound.append({'sent_at':1000000,'text':'waiting'})
        async def sleep(_):
            ch._outbound.clear()
        with patch('plugin.relay.channels.proactive.asyncio.sleep',side_effect=sleep), patch.object(ch,'_check_queue_wake',new_callable=AsyncMock) as check:
            await ch._queue_wake_loop()
            check.assert_awaited_once()
    async def test_adapter_uses_shared_sender_and_uri_action(self):
        proc=type('Process',(),{'returncode':0,'communicate':AsyncMock(return_value=(b'',b''))})()
        with patch('plugin.relay.join_wake.asyncio.create_subprocess_exec',new_callable=AsyncMock,return_value=proc) as create:
            self.assertTrue(await join_wake.wake_queued_phone({'text':'one\ntwo'},3))
        args=create.await_args.args
        self.assertIn('Hermes: 3 waiting',args)
        self.assertIn('one',args)
        self.assertNotIn('one\ntwo',args)
        self.assertIn('relay-queue',args)
        self.assertIn('--actions',args)
