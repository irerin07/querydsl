package study.querydsl;

import static org.assertj.core.api.Assertions.assertThat;
import static study.querydsl.entity.QMember.member;
import static study.querydsl.entity.QTeam.*;

import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.jpa.impl.JPAQueryFactory;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.QTeam;
import study.querydsl.entity.Team;

@SpringBootTest
@Transactional
public class QuerydslBasicTest {

  @Autowired
  EntityManager em;

  JPAQueryFactory queryFactory;

  @BeforeEach
  public void before() {
    queryFactory = new JPAQueryFactory(em);
    Team teamA = new Team("teamA");
    Team teamB = new Team("teamB");
    em.persist(teamA);
    em.persist(teamB);
    Member member1 = new Member("member1", 10, teamA);
    Member member2 = new Member("member2", 20, teamA);
    Member member3 = new Member("member3", 30, teamB);
    Member member4 = new Member("member4", 40, teamB);
    em.persist(member1);
    em.persist(member2);
    em.persist(member3);
    em.persist(member4);
  }

  @Test
  public void startJPQL() {
    //find member1
    Member findMember = em
        .createQuery("select m from Member m where m.username =: username", Member.class)
        .setParameter("username", "member1")
        .getSingleResult();

    assertThat(findMember.getUsername()).isEqualTo("member1");
  }

  @Test
  public void startQueryDsl() {
//    QMember m = new QMember("m");
//    QMember qMember = member;
    Member findMember = queryFactory
        .select(member)
        .from(member)
        .where(member.username.eq("member1"))
        .fetchOne();

    Member member1 = queryFactory
        .selectFrom(member)
        .where(QMember.member.username.eq("member1"))
        .fetchOne();

    assertThat(findMember.getUsername()).isEqualTo("member1");
    System.out.println("findMember: " + findMember);
    assertThat(member1.getUsername()).isEqualTo("member1");
    System.out.println("member: " + member1);
  }

  @Test
  public void search() {
    Member findMember = queryFactory
        .selectFrom(member)
        .where(member.username.eq("member1")
            .and(member.age.eq(10)))
        .fetchOne();

    assertThat(findMember.getUsername()).isEqualTo("member1");
    assertThat(findMember.getAge()).isEqualTo(10);
  }

  @Test
  public void searchAndParam() {
    Member findMember = queryFactory
        .selectFrom(member)
        .where(
            member.username.eq("member1"),
            member.age.eq(10)
        )
        .fetchOne();

    assertThat(findMember.getUsername()).isEqualTo("member1");
    assertThat(findMember.getAge()).isEqualTo(10);
  }

  @Test
  public void resultFetch() {
//    List<Member> fetch = queryFactory
//        .selectFrom(member)
//        .fetch();
//
//    Member fetchOne = queryFactory
//        .selectFrom(QMember.member)
//        .fetchOne();
//
//    Member fetchFirst = queryFactory
//        .selectFrom(QMember.member)
//        .fetchFirst();
//
//    QueryResults<Member> results = queryFactory
//        .selectFrom(member)
//        .fetchResults();
//    results.getTotal();
//    List<Member> content = results.getResults();

    long total = queryFactory
        .selectFrom(member)
        .fetchCount();
  }

  @Test
  public void sort() {
    em.persist(new Member(null, 100));
    em.persist(new Member("member5", 100));
    em.persist(new Member("member6", 100));

    List<Member> fetch = queryFactory
        .selectFrom(member)
        .where(member.age.eq(100))
        .orderBy(member.age.desc(), member.username.asc().nullsLast())
        .fetch();

    Member member5 = fetch.get(0);
    Member member6 = fetch.get(1);
    Member memberNull = fetch.get(2);

    assertThat(member5.getUsername()).isEqualTo("member5");
    assertThat(member6.getUsername()).isEqualTo("member6");
    assertThat(memberNull.getUsername()).isNull();
  }

  @Test
  public void paging() {
    List<Member> memberList = queryFactory
        .selectFrom(member)
        .orderBy(member.username.desc())
        .offset(1)
        .limit(2)
        .fetch();

    assertThat(memberList.size()).isEqualTo(2);
  }

  @Test
  public void paging2() {
    QueryResults<Member> memberQueryResults = queryFactory
        .selectFrom(member)
        .orderBy(member.username.desc())
        .offset(1)
        .limit(2)
        .fetchResults();

    assertThat(memberQueryResults.getTotal()).isEqualTo(4);
    assertThat(memberQueryResults.getLimit()).isEqualTo(2);
    assertThat(memberQueryResults.getOffset()).isEqualTo(1);
    assertThat(memberQueryResults.getResults().size()).isEqualTo(2);
  }

  @Test
  public void aggregation() {
    List<Tuple> tupleList = queryFactory
        .select(member.count(), member.age.sum(), member.age.avg(), member.age.max(),
            member.age.min())
        .from(member)
        .fetch();

    Tuple tuple = tupleList.get(0);
    assertThat(tuple.get(member.count())).isEqualTo(4);
  }

  @Test
  public void group() throws Exception {
    List<Tuple> tuples = queryFactory
        .select(team.name, member.age.avg())
        .from(member)
        .join(member.team, team)
        .groupBy(team.name)
        .fetch();

    Tuple teamA = tuples.get(0);
    Tuple teamB = tuples.get(1);

    assertThat(teamA.get(team.name)).isEqualTo("teamA");
    assertThat(teamB.get(team.name)).isEqualTo("teamB");
    assertThat(teamA.get(member.age.avg())).isEqualTo(15);
    assertThat(teamB.get(member.age.avg())).isEqualTo(35);

  }

  @Test
  public void join() throws Exception {
    List<Member> memberList = queryFactory
        .selectFrom(member)
        .join(member.team, team)
        .where(team.name.eq("teamA"))
        .fetch();

    assertThat(memberList).extracting("username")
        .containsExactly("member1", "member2");
  }

  @Test
  public void theta_join() throws Exception {
    em.persist(new Member("teamA"));
    em.persist(new Member("teamB"));

    List<Member> fetch = queryFactory
        .select(member)
        .from(member, team)
        .where(member.username.eq(team.name))
        .fetch();

    assertThat(fetch)
        .extracting("username")
        .containsExactly("teamA", "teamB");
  }

  @Test
  public void join_on_filtering() throws Exception {
    List<Tuple> results = queryFactory
        .select(member, team)
        .from(member)
        .leftJoin(member.team, team)
        .on(team.name.eq("teamA"))
        .fetch();

    List<Tuple> results2 = queryFactory
        .select(member, team)
        .from(member)
        .join(member.team, team)
        .on(team.name.eq("teamA"))
        .fetch();

    List<Tuple> results3 = queryFactory
        .select(member, team)
        .from(member)
        .join(member.team, team)
        .where(team.name.eq("teamA"))
        .fetch();


    for (Tuple result : results) {
      System.out.println("result = " + result);
    }
    for (Tuple result : results2) {
      System.out.println("result2 = " + result);
    }
    for (Tuple result : results3) {
      System.out.println("result3 = " + result);
    }
  }

  @Test
  public void join_on_no_relation() throws Exception {
    em.persist(new Member("teamA"));
    em.persist(new Member("teamB"));

    List<Tuple> fetch1 = queryFactory
        .select(member, team)
        .from(member)
        .leftJoin(team).on(member.username.eq(team.name))
        .where(member.username.eq(team.name))
        .fetch();
    for (Tuple tuple : fetch1) {
      System.out.println("tuple = " + tuple);
    }
  }

  @PersistenceUnit
  EntityManagerFactory emf;

  @Test
  public void fetchJoinNo() throws Exception {
    em.flush();
    em.clear();

    Member member = queryFactory
        .selectFrom(QMember.member)
        .where(QMember.member.username.eq("member1"))
        .fetchOne();

    boolean loaded = emf.getPersistenceUnitUtil().isLoaded(member.getTeam());
    assertThat(loaded).as("패치 조인 미적용").isFalse();

  }

  @Test
  public void fetchJoinUse() throws Exception {
    em.flush();
    em.clear();

    Member member = queryFactory
        .selectFrom(QMember.member)
        .join(QMember.member.team, team).fetchJoin()
        .where(QMember.member.username.eq("member1"))
        .fetchOne();

    boolean loaded = emf.getPersistenceUnitUtil().isLoaded(member.getTeam());
    assertThat(loaded).as("패치 조인 적용").isTrue();
  }
}
