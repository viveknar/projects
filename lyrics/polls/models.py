from django.db import models
import datetime

# Create your models here.
class Poll(models.Model):
	question = models.CharField(max_length=200)
	pub_date = models.DateTimeField('Date Published')
	
	def __unicode__(self):
		return self.question

	def wasPubToday(self):
		return self.pub_date.date() == datetime.datetime.date()

class Choice(models.Model):
	poll = models.ForeignKey(Poll)
	choice = models.CharField(max_length=200)
	votes = models.IntegerField()
	def __unicode__(self):
		return self.choice

